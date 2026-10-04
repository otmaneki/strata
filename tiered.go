// Package strata implements a two-tier cache: an in-process local cache in
// front of redis, with pubsub-driven invalidation so a write on one node
// evicts the stale copy cached on others.
//
// A warm local Get returns the local tier's own backing array, not a
// copy, copying on every local hit would defeat the point of a lock-free
// local tier. Treat it as read-only; everywhere else (Set, GetOrLoad, a
// redis-hit promotion) copies. See Get's doc comment.
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

// TieredCache implements Cache: an in-process local tier in front of
// redis. Either tier can be disabled, but not both; see WithoutLocalCache
// and WithoutRedis.
type TieredCache struct {
	local     *localCache
	redis     *redis.Client
	localTTL  time.Duration
	remoteTTL time.Duration
	sf        singleflight.Group

	localEnabled  bool
	remoteEnabled bool

	observer Observer // defaults to NoopObserver; set via WithObserver

	sub *redis.PubSub

	localCacheMaxSize       int // default sizing for the local tier, see newLocalCache
	localCacheEvictInterval time.Duration
	codec                   Codec

	stats tieredStats

	// Latency histograms, nil unless set via WithRedisLatencyHistogram,
	// WithLocalLatencyHistogram, WithPubSubLatencyHistogram. Publishes get
	// their own since they're fire-and-forget, not a data read/write.
	redisLatency  Histogram
	localLatency  Histogram
	pubsubLatency Histogram

	// instanceID tags this instance's published invalidations, so its own
	// SubscribeInvalidations loop can skip them: redis pub/sub echoes a
	// publish back to the publisher, and without this a node would evict
	// its own freshly written entry on every Set.
	instanceID string
}

// newInstanceID returns a random id for tagging this instance's published
// invalidations.
func newInstanceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// Option configures optional TieredCache behavior.
type Option func(*TieredCache)

// WithObserver registers an Observer for failure notifications. Default:
// NoopObserver, every event silently dropped.
func WithObserver(o Observer) Option {
	return func(tc *TieredCache) { tc.observer = o }
}

// WithLocalCacheSize caps the local tier at maxSize entries (default
// 1,000,000), see localCache's doc comment for why that bound is best
// effort. No effect with WithoutLocalCache.
func WithLocalCacheSize(maxSize int) Option {
	return func(tc *TieredCache) { tc.localCacheMaxSize = maxSize }
}

// WithLocalCacheEvictInterval sets how often the local tier sweeps for
// expired entries (default 60s). No effect with WithoutLocalCache.
func WithLocalCacheEvictInterval(d time.Duration) Option {
	return func(tc *TieredCache) { tc.localCacheEvictInterval = d }
}

// WithoutLocalCache disables the in-process local tier: every Get and Set
// goes straight to redis. Reach for this when even localTTL of staleness
// is unacceptable.
func WithoutLocalCache() Option {
	return func(tc *TieredCache) { tc.localEnabled = false }
}

// WithoutRedis disables the redis tier: no cross-instance consistency at
// all, not even eventually. The redis client passed to NewTieredCache may
// be nil in this mode; it's never dialed.
func WithoutRedis() Option {
	return func(tc *TieredCache) { tc.remoteEnabled = false }
}

// WithCodec registers a Codec to transform values before they're written
// to redis and after they're read back, e.g. compression. Local-tier
// values are never transformed. No effect with WithoutRedis.
func WithCodec(c Codec) Option {
	return func(tc *TieredCache) { tc.codec = c }
}

// WithRedisLatencyHistogram records every redis data round trip (Get,
// Set, Del), not invalidation publishes, see WithPubSubLatencyHistogram.
// No effect with WithoutRedis.
func WithRedisLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.redisLatency = h }
}

// WithPubSubLatencyHistogram records every invalidation publish, kept
// separate from WithRedisLatencyHistogram since a publish is
// fire-and-forget, not a data read/write. No effect with WithoutRedis.
func WithPubSubLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.pubsubLatency = h }
}

// WithLocalLatencyHistogram records every local tier lookup. Most useful
// for spotting contention under load, not catching a slow individual
// call, a local lookup is a lock-free map read. No effect with
// WithoutLocalCache.
func WithLocalLatencyHistogram(h Histogram) Option {
	return func(tc *TieredCache) { tc.localLatency = h }
}

// NewTieredCache builds a TieredCache backed by rc: redis-promoted values
// are kept locally for localTTL, written values are kept in redis for
// remoteTTL. Call Close when done, to stop the local tier's background
// sweeper. rc may be nil if WithoutRedis is used.
//
// Panics if both WithoutLocalCache and WithoutRedis are given: a cache
// that could never store or retrieve anything is a construction mistake,
// not a runtime condition.
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

// Close stops the local tier's sweeper and any SubscribeInvalidations
// subscription. It does not close the redis client; the caller owns that.
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

// startLocalTimer/observeLocalLatency, and their redis/pubsub
// counterparts below, skip the clock read entirely when no Histogram is
// registered, keeping the With*LatencyHistogram options free for callers
// who don't use them, notably on the hot local-read path.
func (tc *TieredCache) startLocalTimer() time.Time {
	if tc.localLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observeLocalLatency(start time.Time) {
	if tc.localLatency != nil {
		tc.localLatency.Observe(time.Since(start).Seconds())
	}
}

func (tc *TieredCache) startRedisTimer() time.Time {
	if tc.redisLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observeRedisLatency(start time.Time) {
	if tc.redisLatency != nil {
		tc.redisLatency.Observe(time.Since(start).Seconds())
	}
}

func (tc *TieredCache) startPubSubTimer() time.Time {
	if tc.pubsubLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observePubSubLatency(start time.Time) {
	if tc.pubsubLatency != nil {
		tc.pubsubLatency.Observe(time.Since(start).Seconds())
	}
}

// invalidationPayload tags key's pubsub message with this instance's id,
// so SubscribeInvalidations can skip the instance's own publishes.
func (tc *TieredCache) invalidationPayload(key string) string {
	return tc.instanceID + ":" + key
}

// Get checks the local tier first, then redis, promoting a redis hit
// into the local tier. A warm local hit returns the local tier's own
// backing array, not a copy; everything else here returns a fresh one.
func (tc *TieredCache) Get(ctx context.Context, key string) ([]byte, bool) {
	if tc.localEnabled {
		localStart := tc.startLocalTimer()
		val, ok := tc.local.Get(key)
		tc.observeLocalLatency(localStart)
		if ok {
			// Guard the assertion rather than trust it: TieredCache only
			// ever stores []byte here, but a panic on a violated
			// invariant is worse than a spurious miss.
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

	redisStart := tc.startRedisTimer()
	val, err := tc.redis.Get(ctx, key).Bytes()
	tc.observeRedisLatency(redisStart)
	switch {
	case err == nil:
		tc.stats.redisHits.Add(1)
		decoded := val
		if tc.codec != nil {
			decoded, err = tc.codec.Decode(val)
			if err != nil {
				// Fail open, same as a redis error: don't promote
				// undecodable bytes into the local tier.
				tc.stats.decodeErrors.Add(1)
				tc.observer.OnDecodeError(err)
				return nil, false
			}
		}

		if tc.localEnabled {
			// Copy before storing: decoded is also returned below, and
			// a caller mutating it mustn't reach into the local tier.
			localStart := tc.startLocalTimer()
			tc.local.Set(key, bytes.Clone(decoded), tc.localTTL)
			tc.observeLocalLatency(localStart)
		}
		return decoded, true
	case errors.Is(err, redis.Nil):
		tc.stats.redisMisses.Add(1)
		return nil, false
	default:
		// Fail open: report a miss, not an error, but let the Observer
		// know, or a redis outage would look identical to a cold cache.
		tc.stats.redisErrors.Add(1)
		tc.observer.OnRedisError(err)
		return nil, false
	}
}

// Set writes value to both tiers (copying it, safe to reuse value
// afterward), bumps key's version counter in redis for setIfVersion, and
// publishes an invalidation so other instances drop their stale local
// copy. It always applies, unconditionally; see setIfVersion for the
// conditional write GetOrLoad needs instead.
func (tc *TieredCache) Set(ctx context.Context, key string, value []byte) error {
	if tc.localEnabled {
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

	ttlMillis := int64(tc.remoteTTL / time.Millisecond)
	redisStart := tc.startRedisTimer()
	err := setScript.Run(ctx, tc.redis, []string{key, versionKey(key)}, toStore, ttlMillis).Err()
	tc.observeRedisLatency(redisStart)
	if err != nil {
		return err
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// Invalidate deletes key and publishes an invalidation for other
// instances. The redis delete also bumps key's version counter,
// atomically: a setIfVersion call already in flight for this key must
// see that something changed, or it would resurrect the value this just
// deleted.
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
	err := delScript.Run(ctx, tc.redis, []string{key, versionKey(key)}).Err()
	tc.observeRedisLatency(delStart)
	if err != nil {
		return fmt.Errorf("delete %s from redis: %w", key, err)
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// SubscribeInvalidations listens for Set/Invalidate's published
// invalidations and evicts the affected key locally. Call at most once;
// Close stops it. No-op if either tier is disabled.
func (tc *TieredCache) SubscribeInvalidations(ctx context.Context) {
	if !tc.localEnabled || !tc.remoteEnabled {
		return
	}
	tc.sub = tc.redis.Subscribe(ctx, invalidationPubSubChannel)
	go func() {
		for msg := range tc.sub.Channel() {
			origin, key, found := strings.Cut(msg.Payload, ":")
			if !found {
				key = origin // no instance id prefix: treat it all as the key
			} else if origin == tc.instanceID {
				continue // our own publish echoed back; already applied locally
			}
			tc.local.Delete(key)
		}
	}()
}

// GetOrLoad implements Loader for TieredCache.
// On a miss, it reads key's version before running loader, so
// setIfVersion can later tell whether a concurrent write landed while the
// (possibly slow) loader was still running and if so drop the loader's
// now-stale result instead of caching it.
func (tc *TieredCache) GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if val, ok := tc.Get(ctx, key); ok {
		return val, nil
	}

	// Only one goroutine executes per key; others wait and share the result.
	result, err, _ := tc.sf.Do(key, func() (any, error) {
		// Double-check: another goroutine may have filled the cache
		// while this one waited for the singleflight slot.
		if val, ok := tc.Get(ctx, key); ok {
			return val, nil
		}

		// Captured before, not after, loader runs: see setIfVersion.
		var expectedVersion int64
		var versionErr error
		if tc.remoteEnabled {
			expectedVersion, versionErr = tc.currentVersion(ctx, key)
		}

		val, err := loader(ctx)
		if err != nil {
			return nil, fmt.Errorf("loader for key %s: %w", key, err)
		}

		// Fail open: the loader already did the real work, so a
		// cache-population failure is reported via Observer, not
		// returned, and shouldn't fail this call too.
		switch {
		case !tc.remoteEnabled:
			// No concurrent redis writer to race against, so Set's plain
			// last-write-wins is fine.
			if err := tc.Set(ctx, key, val); err != nil {
				tc.stats.setErrors.Add(1)
				tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
			}
		case versionErr != nil:
			tc.stats.setErrors.Add(1)
			tc.observer.OnSetError(fmt.Errorf("read version for key %s: %w", key, versionErr))
		default:
			switch applied, err := tc.setIfVersion(ctx, key, val, expectedVersion); {
			case err != nil:
				tc.stats.setErrors.Add(1)
				tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
			case !applied:
				tc.stats.staleWritesDropped.Add(1)
			}
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
