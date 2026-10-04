# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is `0.x`, the API and the redis data layout may change
between minor versions; every such change is called out here.

## [Unreleased]

### Added

- `TieredCache`: a two-tier cache with an in-process local tier in front of
  redis. `Get` checks the local tier first, falls back to redis, and promotes
  redis hits into the local tier. `NewTieredCache` panics if both tiers are
  disabled at construction time, a cache that could never store or retrieve
  anything is always a construction mistake, not a runtime condition.
- The local tier: a `sync.Map` with an approximate size bound
  (`WithLocalCacheSize`) and a background TTL sweep
  (`WithLocalCacheEvictInterval`). Reads are lock-free; eviction is
  best-effort, not LRU, see "Known limitations".
- `GetOrLoad` with singleflight: concurrent misses for the same key share a
  single loader call.
- Generic helpers `GetOrLoad[T]` and `WithCache`, with a pluggable
  `Marshaler[T]` (`JSONMarshaler[T]` by default).
- Cross-instance invalidation over redis pub/sub via `Invalidate` and
  `SubscribeInvalidations`. `Set` also publishes an invalidation, so other
  instances drop their stale local copy after a write.
- Instance IDs on published invalidations, so an instance skips its own
  messages instead of evicting the value it just wrote.
- Per-key version counters in redis, used by `GetOrLoad` to detect a `Set`,
  `Invalidate`, or another load that happened while its loader was running.
  In that case the loaded value is still returned to the caller but is not
  cached, so a slow loader can no longer write a stale value back into redis.
  The counter is redis's own, incremented atomically alongside the write it
  guards; ordering never depends on any instance's clock.
- `Set`, `GetOrLoad`'s cache write, and a redis-hit promotion inside `Get`
  all copy the value into the local tier rather than retain the caller's or
  loader's own slice, so reusing or mutating a buffer (e.g. one pulled from
  a `sync.Pool`) right after the call is safe. The one exception is a warm
  local `Get`, see "Known limitations".
- A pluggable `Codec` (`WithCodec`) for transforming values before they hit
  redis and after they come back, e.g. compression. It only applies to the
  redis tier; the local tier always holds the original, decoded value.
- `WithoutRedis` and `WithoutLocalCache` to run with only one tier.
- `Observer` interface (with embeddable `NoopObserver`), registered via
  `WithObserver`, notified on redis, encode, decode, and background set
  failures, all of which fail open.
- `Stats()` snapshot of hit, miss, and error counters, including
  `StaleWritesDropped`.
- A minimal `Histogram` interface (matching `prometheus/client_golang`'s
  `Observe` method, no adapter needed) and three options to wire one up:
  `WithLocalLatencyHistogram`, `WithRedisLatencyHistogram`, and
  `WithPubSubLatencyHistogram`, kept separate since the three have
  different scales and failure modes.
- Small composable interfaces: `Reader`, `Writer`, `ReadWriter`, `Loader`,
  `Invalidator`, and `Cache`.
- MIT license.

### Redis data layout

- Values are stored under the caller's key exactly as given (after the
  `Codec`, if one is configured), with nothing prepended or wrapped.
- Each written key has a version counter at `strata:ver:<key>`. Keys with
  this prefix are reserved for strata.
- Invalidations are published on the `cache:invalidate` channel with the
  payload `<instance-id>:<key>`.

### Known limitations

- Version counters never expire, so redis memory grows with the number of
  distinct keys ever written. Under an `allkeys-*` eviction policy, redis may
  evict them anyway.
- Redis Cluster is not supported yet: only `*redis.Client` is accepted, and
  a value key and its version key may hash to different slots.
- Pub/sub delivers each message at most once. Invalidations published while
  a subscriber is disconnected are lost, and affected local entries stay
  stale for up to `localTTL`.
- A warm local `Get` returns the cached slice itself, not a copy. Treat it
  as read-only.
- The local tier' size bound and eviction are best-effort, not exact:
  `sync.Map` doesn't track access order, so there's no LRU, the victim is
  whichever entry `Range` visits first, and the grow-then-evict sequence in
  `Set` isn't atomic under concurrent writers.
- `GetOrLoad` issues one extra redis round trip per cache miss, to read the
  key's version before running the loader, on top of the round trip the
  miss check and the eventual write already cost.

[Unreleased]: https://github.com/otmaneki/strata/commits/main
