// Package strata implements a two-tier cache: an in-process local cache in
// front of redis, with pubsub-driven invalidation so a write on one node
// evicts the stale copy cached on others.
package strata

import (
	"context"
	"errors"
	"fmt"
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
// can be disabled — see WithoutLocalCache and WithoutRedis — but not both.
type TieredCache struct {
	local     *localCache
	redis     *redis.Client
	localTTL  time.Duration
	remoteTTL time.Duration
	sf        singleflight.Group

	localEnabled  bool
	remoteEnabled bool

	// observer receives failure notifications — see Observer. Defaults to
	// NoopObserver; set via WithObserver.
	observer Observer

	sub *redis.PubSub

	// localCacheMaxSize and localCacheEvictInterval size the local tier.
	// They're read once, in NewTieredCache, to construct it — set via
	// WithLocalCacheSize / WithLocalCacheEvictInterval, not directly.
	localCacheMaxSize       int
	localCacheEvictInterval time.Duration
	codec                   Codec
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
// never invalidated by, any other instance — there's no cross-instance
// consistency at all, not even eventually. The redis client passed to
// NewTieredCache may be nil in this mode; it's never dialed.
func WithoutRedis() Option {
	return func(tc *TieredCache) { tc.remoteEnabled = false }
}

// WithCodec registers a Codec used to transform values before they're
// written to redis and after they're read back — e.g. to compress payloads
// over the wire. It only applies to the redis tier: the local tier always
// holds the original, decoded value, so a warm local Get never pays the
// encode/decode cost. Without this option, values are stored as-is. No
// effect if WithoutRedis is also used.
func WithCodec(c Codec) Option {
	return func(tc *TieredCache) { tc.codec = c }
}

// NewTieredCache builds a TieredCache backed by rc: values promoted from
// redis into the local tier are kept for localTTL, values written to redis
// are kept for remoteTTL. It starts the local tier's background TTL
// sweeper immediately, unless WithoutLocalCache is used — call Close when
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
	}
	for _, opt := range opts {
		opt(tc)
	}
	if !tc.localEnabled && !tc.remoteEnabled {
		//nolint:forbidigo // construction-time misconfiguration, not a runtime condition — same class as regexp.MustCompile
		panic("cache: WithoutLocalCache and WithoutRedis together disable both tiers — TieredCache would never store or retrieve anything")
	}
	if tc.localEnabled {
		tc.local = newLocalCache(tc.localCacheMaxSize, tc.localCacheEvictInterval)
	}
	return tc
}

// Close releases resources owned by the TieredCache: the local tier's TTL
// sweeper (if it has one), and the pubsub subscription if
// SubscribeInvalidations was called. It does not close the redis client,
// since TieredCache doesn't own it — the caller constructed it and is
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

// Get looks up key, checking the local tier first (unless WithoutLocalCache
// was used) and falling back to redis (unless WithoutRedis was used). A
// redis hit is promoted into the local tier so the next Get for the same
// key is served locally.
func (tc *TieredCache) Get(ctx context.Context, key string) ([]byte, bool) {
	// Tier 1: Local memory
	if tc.localEnabled {
		if val, ok := tc.local.Get(key); ok {
			// TieredCache only ever stores []byte in the local tier (via
			// Set and the redis-hit promotion below), so this assertion is
			// safe under normal use; guard it anyway rather than risk a
			// panic.
			if b, ok := val.([]byte); ok {
				return b, true
			}
			return nil, false
		}
	}

	if !tc.remoteEnabled {
		return nil, false
	}

	// Tier 2: Redis
	val, err := tc.redis.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		decoded := val
		if tc.codec != nil {
			decoded, err = tc.codec.Decode(val)
			if err != nil {
				tc.observer.OnDecodeError(err)
				// Fail open, same as a redis error: don't promote
				// undecodable bytes into the local tier.
				return nil, false
			}
		}

		if tc.localEnabled {
			// Local always holds the decoded value — never the encoded
			// wire format — so a later warm Get doesn't need to decode
			// again and doesn't return raw/compressed bytes to the
			// caller.
			tc.local.Set(key, decoded, tc.localTTL)
		}
		return decoded, true
	case errors.Is(err, redis.Nil):
		// Genuine miss: the key doesn't exist in redis either.
		return nil, false
	default:
		// Something other than a miss — timeout, connection error, etc.
		// We still report this as a miss (failing open is the right
		// default for a cache), but let an observer know it happened.
		tc.observer.OnRedisError(err)
		return nil, false
	}
}

// Set writes value under key to whichever tiers are enabled. The local tier
// always stores value as-is; if a Codec is configured, the redis tier
// stores the encoded form instead.
func (tc *TieredCache) Set(ctx context.Context, key string, value []byte) error {
	if tc.localEnabled {
		tc.local.Set(key, value, tc.localTTL)
	}

	if !tc.remoteEnabled {
		return nil
	}

	toStore := value
	if tc.codec != nil {
		encoded, err := tc.codec.Encode(value)
		if err != nil {
			tc.observer.OnEncodeError(err)
			return fmt.Errorf("encode value for key %s: %w", key, err)
		}
		toStore = encoded
	}

	return tc.redis.Set(ctx, key, toStore, tc.remoteTTL).Err()
}

// Invalidate removes key from whichever tiers are enabled on this node and,
// if both tiers are enabled, publishes an invalidation so other nodes
// subscribed via SubscribeInvalidations drop their local copy too. It
// returns before publishing if the redis delete itself fails, since
// announcing an invalidation for a key that's still live in redis would be
// misleading.
func (tc *TieredCache) Invalidate(ctx context.Context, key string) error {
	if tc.localEnabled {
		tc.local.Delete(key)
	}

	if !tc.remoteEnabled {
		return nil
	}

	if err := tc.redis.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("delete %s from redis: %w", key, err)
	}

	return tc.redis.Publish(ctx, invalidationPubSubChannel, key).Err()
}

// SubscribeInvalidations starts listening for invalidations published by
// Invalidate — on this node or others — and evicts the affected key from
// the local tier. Call it at most once per TieredCache; call Close to stop
// it.
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
			tc.local.Delete(msg.Payload)
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

		val, err := loader(ctx)
		if err != nil {
			return nil, fmt.Errorf("loader for key %s: %w", key, err)
		}

		// Fail open: the loader already did the real work and val is
		// good, so a cache-population failure shouldn't fail this call
		// too. Report it via the observer instead of the return value —
		// see Observer.OnSetError's doc comment.
		if err := tc.Set(ctx, key, val); err != nil {
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
