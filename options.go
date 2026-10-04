package strata

import "time"

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

// WithLoaderTimeout bounds how long GetOrLoad's shared loader call may
// run (default 30s). It applies to the detached context the loader runs
// on, not the caller's own; see GetOrLoad's doc comment for why that
// context is detached in the first place. 0 disables the bound.
func WithLoaderTimeout(timeout time.Duration) Option {
	return func(tc *TieredCache) { tc.loaderTimeout = timeout }
}
