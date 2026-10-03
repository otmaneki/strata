// Package strata implements a two-tier cache: an in-process local cache in
// front of redis, with pubsub-driven invalidation so a write on one node
// evicts the stale copy cached on others.
//
// Set, GetOrLoad's cache write, and a redis-hit promotion inside Get all
// store their own copy in the local tier, so mutating or reusing a buffer
// (e.g. one pulled from a sync.Pool) after handing it to this package, or
// mutating a []byte this package just handed back, is safe: neither can
// reach back into what's cached. The one deliberate exception is a warm
// local hit inside Get: it returns the local tier's own backing array
// directly, not a copy, because copying on every local hit would undo
// most of the point of having a lock-free local tier at all. Mutating
// that particular returned slice in place does corrupt the cached value
// for every other caller that hits the same key afterward, silently,
// until the key is next overwritten or evicted. Treat it as read-only,
// and copy it yourself, e.g. with bytes.Clone, before mutating it. The
// generic GetOrLoad[T]/WithCache layer doesn't have this problem either
// way: Marshaler.Unmarshal decodes into a fresh T on every call.
package strata

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const invalidationPubSubChannel = "cache:invalidate"

// Default sizing for the local tier every TieredCache starts.
// Override via WithLocalCacheSize and WithLocalCacheEvictInterval.
const (
	defaultLocalCacheMaxSize       = 1_000_000
	defaultLocalCacheEvictInterval = 60 * time.Second
)

// TieredCache is a two-tier cache: an in-process local cache in front of
// redis, with pubsub-driven invalidation so a write on one node can evict
// the stale copy cached on others (see SubscribeInvalidations). Either tier
// can be disabled, but not both. See WithoutLocalCache and WithoutRedis.
type TieredCache struct {
	local     *localCache
	redis     *redis.Client
	localTTL  time.Duration
	remoteTTL time.Duration
	sf        singleflight.Group

	localEnabled  bool
	remoteEnabled bool

	// observer receives failure notifications. See Observer for details. Defaults to
	// NoopObserver; set via WithObserver.
	observer Observer

	sub *redis.PubSub

	// localCacheMaxSize and localCacheEvictInterval size the local tier.
	// They're read once, in NewTieredCache, to construct it. Set via
	// WithLocalCacheSize / WithLocalCacheEvictInterval, not directly.
	localCacheMaxSize       int
	localCacheEvictInterval time.Duration
	codec                   Codec

	stats tieredStats

	// redisLatency and localLatency, if set, receive a latency observation
	// in seconds for every redis data round trip (Get, Set, Del) and every
	// local tier lookup, respectively. pubsubLatency, if set, receives one
	// for every invalidation publish instead, kept separate since a
	// publish is a fire-and-forget broadcast, not a data read/write, and
	// usually has different latency characteristics worth tracking on its
	// own. All three are nil by default, meaning nothing is recorded. Set
	// via WithRedisLatencyHistogram, WithLocalLatencyHistogram, and
	// WithPubSubLatencyHistogram.
	redisLatency  Histogram
	localLatency  Histogram
	pubsubLatency Histogram

	// instanceID tags every invalidation this instance publishes, so its
	// own SubscribeInvalidations loop can tell its own publishes apart
	// from another instance's and skip them. Without this, a node that
	// both writes and subscribes would evict its own freshly written local
	// entry on every Set, since redis pub/sub delivers a publish back to
	// the publisher's own subscription too. Generated once in
	// NewTieredCache.
	instanceID string
}

// newInstanceID returns a random identifier for tagging this TieredCache's
// published invalidations. crypto/rand.Read failing is effectively
// unreachable on supported platforms, the fallback just keeps construction
// from panicking over it rather than producing a meaningfully better id.
func newInstanceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// Option configures optional TieredCache behavior.
type Option func(*TieredCache)

// WithObserver registers an Observer to receive TieredCache's failure
// notifications. Without this option, a TieredCache uses NoopObserver and
// every event is silently dropped.
func WithObserver(o Observer) Option {
	return func(tc *TieredCache) { tc.observer = o }
}

// WithLocalCacheSize sets the maximum number of entries the local tier
// holds before it starts evicting to make room for new keys (default
// 1,000,000). See the local tier's doc comment in cache.go for why this
// bound is best effort rather than exact. No effect if WithoutLocalCache is
// also used.
func WithLocalCacheSize(maxSize int) Option {
	return func(tc *TieredCache) { tc.localCacheMaxSize = maxSize }
}

// WithLocalCacheEvictInterval sets how often the local tier sweeps for
// TTL-expired entries (default 60s). A shorter interval reclaims expired
// memory sooner at the cost of more frequent full scans of the local tier.
// No effect if WithoutLocalCache is also used.
func WithLocalCacheEvictInterval(d time.Duration) Option {
	return func(tc *TieredCache) { tc.localCacheEvictInterval = d }
}

// WithoutLocalCache disables the in-process local tier: every Get and Set
// goes straight to redis, and nothing is ever held in per-instance memory.
// Reach for this when even localTTL of staleness, or per-instance memory
// use, is unacceptable, and paying a redis round trip on every access is
// the better trade.
func WithoutLocalCache() Option {
	return func(tc *TieredCache) { tc.localEnabled = false }
}

// WithoutRedis disables the redis tier: Get and Set only ever touch the
// in-process local tier. This instance's cache is then invisible to, and
// never invalidated by, any other instance. There's no cross-instance
// consistency at all, not even eventually. The redis client passed to
// NewTieredCache may be nil in this mode; it's never dialed.
func WithoutRedis() Option {
	return func(tc *TieredCache) { tc.remoteEnabled = false }
}

// WithCodec registers a Codec used to transform values before they're
// written to redis and after they're read back, e.g. to compress payloads
// over the wire. It only applies to the redis tier: the local tier always
// holds the original, decoded value, so a warm local Get never pays the
// encode/decode cost. Without this option, values are stored as-is. No
// effect if WithoutRedis is also used.
func WithCodec(c Codec) Option {
	return func(tc *TieredCache) { tc.codec = c }
}

// WithRedisLatencyHistogram registers a Histogram that records how long
// each redis data round trip takes, in seconds: every Get, Set, and Del
// call. It does not include invalidation publishes, see
// WithPubSubLatencyHistogram for those. Without this option, redis latency
// isn't recorded anywhere. No effect if WithoutRedis is also used.
func WithRedisLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.redisLatency = h }
}

// WithPubSubLatencyHistogram registers a Histogram that records how long
// each invalidation publish takes, in seconds: the Publish call inside Set
// and Invalidate. Kept separate from WithRedisLatencyHistogram since a
// publish is a fire-and-forget broadcast rather than a data read/write, and
// often behaves differently under load. Without this option, publish
// latency isn't recorded anywhere. No effect if WithoutRedis is also used.
func WithPubSubLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.pubsubLatency = h }
}

// WithLocalLatencyHistogram registers a Histogram that records how long
// each local tier lookup takes, in seconds: every Get, Set, and Delete
// call. A local lookup is a lock-free map read, so individual values are
// tiny, this is mainly useful for spotting contention under heavy
// concurrent load rather than catching a slow individual call. Without
// this option, local latency isn't recorded anywhere. No effect if
// WithoutLocalCache is also used.
func WithLocalLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.localLatency = h }
}

// NewTieredCache builds a TieredCache backed by rc: values promoted from
// redis into the local tier are kept for localTTL, values written to redis
// are kept for remoteTTL. It starts the local tier's background TTL
// sweeper immediately, unless WithoutLocalCache is used. Call Close when
// done to stop it. rc may be nil if WithoutRedis is used.
//
// Panics if both WithoutLocalCache and WithoutRedis are used together: a
// TieredCache with neither tier would never store or retrieve anything,
// which is always a construction mistake, not a runtime condition.
func NewTieredCache(rc *redis.Client, localTTL, remoteTTL time.Duration, opts ...Option) *TieredCache {
	tc := &TieredCache{
		redis:                   rc,
		localTTL:                localTTL,
		remoteTTL:               remoteTTL,
		localCacheMaxSize:       defaultLocalCacheMaxSize,
		localCacheEvictInterval: defaultLocalCacheEvictInterval,
		localEnabled:            true,
		remoteEnabled:           true,
		observer:                NoopObserver{},
		instanceID:              newInstanceID(),
	}
	for _, opt := range opts {
		opt(tc)
	}
	if !tc.localEnabled && !tc.remoteEnabled {
		//nolint:forbidigo // construction-time misconfiguration, not a runtime condition, same class as regexp.MustCompile
		panic("cache: WithoutLocalCache and WithoutRedis together disable both tiers. TieredCache would never store or retrieve anything")
	}
	if tc.localEnabled {
		tc.local = newLocalCache(tc.localCacheMaxSize, tc.localCacheEvictInterval)
	}
	return tc
}

// Close releases resources owned by the TieredCache: the local tier's TTL
// sweeper (if it has one), and the pubsub subscription if
// SubscribeInvalidations was called. It does not close the redis client,
// since TieredCache doesn't own it. The caller constructed it and is
// responsible for it.
func (tc *TieredCache) Close() error {
	if tc.local != nil {
		tc.local.Close()
	}
	if tc.sub != nil {
		return tc.sub.Close()
	}
	return nil
}

// Stats returns a snapshot of the current hit/miss/error counters. See
// Stats's doc comment for what each field means and how to read them.
func (tc *TieredCache) Stats() Stats {
	return tc.stats.snapshot()
}

// startLocalTimer returns the current time if a local latency Histogram is
// registered, or the zero time otherwise. Pair it with observeLocalLatency.
// Skipping the clock read entirely when no Histogram is registered keeps
// WithLocalLatencyHistogram's cost at zero on the hot local-read path for
// callers who never opt into it.
func (tc *TieredCache) startLocalTimer() time.Time {
	if tc.localLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

// observeLocalLatency reports how long a local tier call took, given a
// start time from startLocalTimer. A no-op if no local latency Histogram is
// registered.
func (tc *TieredCache) observeLocalLatency(start time.Time) {
	if tc.localLatency != nil {
		tc.localLatency.Observe(time.Since(start).Seconds())
	}
}

// startRedisTimer is startLocalTimer's redis-tier counterpart, see its doc
// comment.
func (tc *TieredCache) startRedisTimer() time.Time {
	if tc.redisLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

// observeRedisLatency reports how long a redis call took, given a start
// time from startRedisTimer. A no-op if no redis latency Histogram is
// registered.
func (tc *TieredCache) observeRedisLatency(start time.Time) {
	if tc.redisLatency != nil {
		tc.redisLatency.Observe(time.Since(start).Seconds())
	}
}

// startPubSubTimer is startLocalTimer's invalidation-publish counterpart,
// see its doc comment.
func (tc *TieredCache) startPubSubTimer() time.Time {
	if tc.pubsubLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

// observePubSubLatency reports how long an invalidation publish took,
// given a start time from startPubSubTimer. A no-op if no pub/sub latency
// Histogram is registered.
func (tc *TieredCache) observePubSubLatency(start time.Time) {
	if tc.pubsubLatency != nil {
		tc.pubsubLatency.Observe(time.Since(start).Seconds())
	}
}

// invalidationPayload builds the pubsub message published for key,
// prefixed with this instance's id so SubscribeInvalidations can recognize
// and skip its own publishes. See instanceID's doc comment.
func (tc *TieredCache) invalidationPayload(key string) string {
	return tc.instanceID + ":" + key
}

// Get looks up key, checking the local tier first (unless WithoutLocalCache
// was used) and falling back to redis (unless WithoutRedis was used). A
// redis hit is promoted into the local tier so the next Get for the same
// key is served locally.
//
// A warm local hit returns the local tier's own backing array directly,
// not a copy, see the package doc comment. A redis hit doesn't have this
// problem, that []byte is freshly decoded for this call alone.
func (tc *TieredCache) Get(ctx context.Context, key string) ([]byte, bool) {
	// Tier 1: Local memory
	if tc.localEnabled {
		localStart := tc.startLocalTimer()
		val, ok := tc.local.Get(key)
		tc.observeLocalLatency(localStart)
		if ok {
			// TieredCache only ever stores []byte in the local tier (via
			// Set and the redis-hit promotion below), so this assertion is
			// safe under normal use; guard it anyway rather than risk a
			// panic.
			if b, ok := val.([]byte); ok {
				tc.stats.localHits.Add(1)
				return b, true
			}
			return nil, false
		}
		tc.stats.localMisses.Add(1)
	}

	if !tc.remoteEnabled {
		return nil, false
	}

	// Tier 2: Redis
	redisStart := tc.startRedisTimer()
	val, err := tc.redis.Get(ctx, key).Bytes()
	tc.observeRedisLatency(redisStart)
	switch {
	case err == nil:
		tc.stats.redisHits.Add(1)
		data := stripVersion(val)
		decoded := data
		if tc.codec != nil {
			decoded, err = tc.codec.Decode(data)
			if err != nil {
				tc.stats.decodeErrors.Add(1)
				tc.observer.OnDecodeError(err)
				// Fail open, same as a redis error: don't promote
				// undecodable bytes into the local tier.
				return nil, false
			}
		}

		if tc.localEnabled {
			// Local always holds the decoded value, never the encoded
			// wire format, so a later warm Get doesn't need to decode
			// again and doesn't return raw/compressed bytes to the
			// caller. Stored as its own copy, same reasoning as Set: the
			// caller is about to get this exact decoded slice back too,
			// and mutating a returned slice must not reach back into
			// what's now cached.
			localStart := tc.startLocalTimer()
			tc.local.Set(key, bytes.Clone(decoded), tc.localTTL)
			tc.observeLocalLatency(localStart)
		}
		return decoded, true
	case errors.Is(err, redis.Nil):
		// Genuine miss: the key doesn't exist in redis either.
		tc.stats.redisMisses.Add(1)
		return nil, false
	default:
		// Something other than a miss, e.g. a timeout or connection error.
		// We still report this as a miss (failing open is the right
		// default for a cache), but let an observer know it happened.
		tc.stats.redisErrors.Add(1)
		tc.observer.OnRedisError(err)
		return nil, false
	}
}

// Set writes value under key to whichever tiers are enabled. The local
// tier stores its own copy of value, so it's safe to mutate or reuse
// value (e.g. a buffer pulled from a sync.Pool) after Set returns. If a
// Codec is configured, the redis tier stores the encoded form instead,
// tagged with the current time so a slower concurrent write never
// clobbers it, see setIfNewer's doc comment.
// If both tiers are enabled, it also publishes an invalidation, the same
// one Invalidate sends, so other nodes subscribed via
// SubscribeInvalidations drop their now-stale local copy instead of
// serving it until localTTL expires. It returns before publishing if the
// redis write itself fails, since announcing an invalidation for a write
// that never happened would be misleading.
//
// Set itself always applies, regardless of ordering: it's an explicit
// request to store this value now, not a conditional one. The version tag
// exists so a later GetOrLoad call, specifically one whose loader was
// already running when this Set happened, can tell its own result is now
// stale and avoid overwriting what Set just wrote.
func (tc *TieredCache) Set(ctx context.Context, key string, value []byte) error {
	if tc.localEnabled {
		// A defensive copy: value may be a buffer the caller reuses (e.g.
		// pulled from a sync.Pool) or mutates after this call returns, and
		// the local tier retains whatever it's given directly rather than
		// copying it itself, see cache.go's localCache doc comment.
		// Cloning once here, instead of requiring every caller to copy
		// before calling Set, is what makes Set safe to call with a
		// buffer the caller doesn't exclusively own anymore afterward.
		localStart := tc.startLocalTimer()
		tc.local.Set(key, bytes.Clone(value), tc.localTTL)
		tc.observeLocalLatency(localStart)
	}

	if !tc.remoteEnabled {
		return nil
	}

	toStore := value
	if tc.codec != nil {
		encoded, err := tc.codec.Encode(value)
		if err != nil {
			tc.stats.encodeErrors.Add(1)
			tc.observer.OnEncodeError(err)
			return fmt.Errorf("encode value for key %s: %w", key, err)
		}
		toStore = encoded
	}

	redisStart := tc.startRedisTimer()
	err := tc.redis.Set(ctx, key, versionedPayload(time.Now().UnixNano(), toStore), tc.remoteTTL).Err()
	tc.observeRedisLatency(redisStart)
	if err != nil {
		return err
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// Invalidate removes key from whichever tiers are enabled on this node and,
// if both tiers are enabled, publishes an invalidation so other nodes
// subscribed via SubscribeInvalidations drop their local copy too. It
// returns before publishing if the redis delete itself fails, since
// announcing an invalidation for a key that's still live in redis would be
// misleading.
func (tc *TieredCache) Invalidate(ctx context.Context, key string) error {
	if tc.localEnabled {
		localStart := tc.startLocalTimer()
		tc.local.Delete(key)
		tc.observeLocalLatency(localStart)
	}

	if !tc.remoteEnabled {
		return nil
	}

	delStart := tc.startRedisTimer()
	err := tc.redis.Del(ctx, key).Err()
	tc.observeRedisLatency(delStart)
	if err != nil {
		return fmt.Errorf("delete %s from redis: %w", key, err)
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// SubscribeInvalidations starts listening for invalidations published by
// Set and Invalidate, on this node or others, and evicts the affected key
// from the local tier. Call it at most once per TieredCache; call Close to
// stop it.
//
// It's a no-op if either tier is disabled: with no local tier there's
// nothing to invalidate locally, and with no redis tier there's no pubsub
// channel to subscribe to.
func (tc *TieredCache) SubscribeInvalidations(ctx context.Context) {
	if !tc.localEnabled || !tc.remoteEnabled {
		return
	}
	tc.sub = tc.redis.Subscribe(ctx, invalidationPubSubChannel)
	go func() {
		for msg := range tc.sub.Channel() {
			origin, key, found := strings.Cut(msg.Payload, ":")
			if !found {
				// No instance id prefix, treat the whole payload as the
				// key rather than drop it.
				key = origin
			} else if origin == tc.instanceID {
				// This instance published it: redis pub/sub delivers a
				// publish back to the publisher's own subscription, and
				// Set/Invalidate already applied the change locally
				// before publishing, so there's nothing to do here.
				continue
			}
			tc.local.Delete(key)
		}
	}()
}

// GetOrLoad gets a key from the cache and if it doesn't exists it will invoke the loader function to fetch it set it in the cache
// then return you back the result, and here is how to use it:
//
//	data, err := tc.GetOrLoad(ctx, "user:1234", func(ctx context.Context) ([]byte, error) {
//	    u, err := db.GetUser(ctx, 1234)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return json.Marshal(u)
//	})
//
// The []byte loader returns is copied before it's cached, and so is the
// []byte Set itself ever stores; see the package doc comment for the one
// remaining exception, a warm hit served straight from Get.
func (tc *TieredCache) GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if val, ok := tc.Get(ctx, key); ok {
		return val, nil
	}

	// Only one goroutine executes per key; others wait and share the result.
	result, err, _ := tc.sf.Do(key, func() (any, error) {
		// Double-check: Another goroutine may have filled the cache
		// while we waited for the singleflight slot.
		if val, ok := tc.Get(ctx, key); ok {
			return val, nil
		}

		// Recorded before the loader runs, not after: the loader can be
		// slow, and if a Set for this key lands while it's still running,
		// that Set's version will be newer. Stamping the load's version
		// from before it started, rather than after it finished, is what
		// lets setIfNewer tell the two apart and reject the stale one.
		loadStart := time.Now().UnixNano()
		val, err := loader(ctx)
		if err != nil {
			return nil, fmt.Errorf("loader for key %s: %w", key, err)
		}

		// Fail open: the loader already did the real work and val is
		// good, so a cache-population failure shouldn't fail this call
		// too. Report it via the observer instead of the return value,
		// see Observer.OnSetError's doc comment.
		if tc.remoteEnabled {
			// setIfNewer, not Set: a concurrent write for this key that
			// completed while the loader was still running must win, see
			// its doc comment. WithoutRedis has no such concurrent writer
			// to race against, local tier writes are already
			// last-write-wins there, so Set's plain behavior is fine.
			switch applied, err := tc.setIfNewer(ctx, key, val, loadStart); {
			case err != nil:
				tc.stats.setErrors.Add(1)
				tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
			case !applied:
				tc.stats.staleWritesDropped.Add(1)
			}
		} else if err := tc.Set(ctx, key, val); err != nil {
			tc.stats.setErrors.Add(1)
			tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
		}

		return val, nil
	})
	if err != nil {
		return nil, err
	}

	// result is always []byte: it's either what Get returned or what loader
	// returned, both of which are []byte by construction. Guard it anyway
	// rather than risk a panic.
	b, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T for key %s", result, key)
	}
	return b, nil
}
