# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is `0.x`, the API and the redis data layout may change
between minor versions; every such change is called out here.

## [Unreleased]

### Added

- `TieredCache`: local tier first, redis fallback with promotion.
  `NewTieredCache` panics if both tiers are disabled.
- The local tier: `sync.Map`, approximate size bound
  (`WithLocalCacheSize`), background TTL sweep
  (`WithLocalCacheEvictInterval`). Eviction is best-effort, not LRU, see
  "Known limitations".
- `GetOrLoad` with singleflight dedup. A caller's own context canceling
  doesn't abort the shared loader call other waiters depend on; it runs
  on a detached context bounded by `WithLoaderTimeout` (default 30s)
  instead. A panicking loader is recovered and returned as an error, not
  a process crash.
- Generic `GetOrLoad[T]` and `WithCache`, with a pluggable `Marshaler[T]`
  (`JSONMarshaler[T]` default).
- Cross-instance invalidation: `Invalidate`/`SubscribeInvalidations` over
  redis pub/sub; `Set` publishes too. Published messages carry an
  instance id so an instance skips its own.
- Per-key version counters in redis: `GetOrLoad` drops a loader's result
  instead of caching it if a `Set`, `Invalidate`, or another load touched
  the key while it was running. The counter is redis's own, immune to
  clock skew between instances.
- `Set`, `GetOrLoad`'s cache write, and a redis-hit promotion in `Get`
  copy the value into the local tier, safe to reuse a buffer (e.g. from a
  `sync.Pool`) right after the call. Exception: a warm local `Get`, see
  "Known limitations".
- `Codec` (`WithCodec`): transforms values for the redis tier only, e.g.
  compression.
- `WithoutRedis`, `WithoutLocalCache`.
- `Observer` (`WithObserver`, embeddable `NoopObserver`): notified on
  redis, encode, decode, and set failures, all fail open.
- `Stats()`: hit/miss/error counters, including `StaleWritesDropped`.
- `Histogram` (matches `prometheus/client_golang`'s `Observe`), wired up
  via `WithLocalLatencyHistogram`, `WithRedisLatencyHistogram`,
  `WithPubSubLatencyHistogram`.
- `Reader`, `Writer`, `ReadWriter`, `Loader`, `Invalidator`, `Cache`.
- MIT license.

### Redis data layout

- Values are stored under the caller's key exactly as given (after the
  `Codec`, if configured); nothing prepended or wrapped.
- Each key has a version counter at `strata:ver:{<key>}`, hash-tagged so
  it shares a Cluster slot with the plain value key; reserved prefix.
- Invalidations publish on `cache:invalidate` as `<instance-id>:<key>`.

### Known limitations

- Version counters never expire; redis memory grows with the number of
  distinct keys ever written. An `allkeys-*` eviction policy may evict
  them anyway.
- `NewTieredCache` accepts `redis.UniversalClient` (`*redis.Client`,
  `*redis.ClusterClient`, or `*redis.Ring`). Against Cluster, the
  version-counter key is hash-tagged to colocate with the value key's
  slot, except when the caller's own key already contains a `{...}` hash
  tag of its own, in which case colocation isn't guaranteed.
- Pub/sub delivers at most once; invalidations published while a
  subscriber is disconnected are lost, local entries stay stale for up to
  `localTTL`.
- A warm local `Get` returns the cached slice itself, not a copy; treat
  it as read-only.
- The local tier's eviction is best-effort, not LRU: `sync.Map` doesn't
  track access order, and the grow-then-evict sequence in `Set` isn't
  atomic under concurrent writers.
- `GetOrLoad` costs one extra redis round trip per miss, to read the
  key's version before running the loader.

### Notes for anyone tracking `main` before this release

- Commit `38add6c` briefly stored values as `<nanoseconds>:<data>`.
  Values written by that commit read back with the prefix still
  attached; flush them or let them expire before upgrading.

[Unreleased]: https://github.com/otmaneki/strata/commits/main
