# strata

A two-tier cache for Go: an in-process local cache sits in front of redis,
with pub/sub-driven invalidation so a write on one node evicts the stale
copy cached on others.

The name comes from geology. Strata are the layers of rock that build up
over time, each one sitting on top of the one below it. A `Get` here works
the same way: it checks the local layer first, and falls through to the
redis layer beneath it only if the value isn't there yet.

## Why

A plain redis cache means every `Get` pays a network round trip, even for
data that's read constantly and barely ever changes. A plain in-process
cache means every instance of your service can disagree about what's
cached, with no way to invalidate the others when something changes.

strata is the combination: reads are served from process memory whenever
possible (sub-100ns, zero allocations), redis is the shared, durable
fallback and the source of truth other instances agree on, and a `Set` or
`Invalidate` call on one instance propagates to the local cache of every
other instance subscribed to it. You get the speed of a local cache with
the consistency guarantees of a shared one, without hand-rolling the
promotion/invalidation logic yourself.

## Features

- **Two tiers, one API.** `Get`/`Set` transparently check local first, fall
  back to redis, and promote redis hits into the local tier.
- **Safe to reuse your buffers.** `Set`, and the `[]byte` a `GetOrLoad`
  loader returns, are always copied before they're cached, so a buffer
  pulled from e.g. a `sync.Pool` is safe to reuse the moment the call
  returns. The one deliberate exception is a warm `Get`, which stays
  zero-copy for speed, see "Copying semantics" below.
- **Cross-instance invalidation.** `Set` and `Invalidate` both publish an
  eviction over redis pub/sub; `SubscribeInvalidations` picks those up on
  every other instance, so a write on one node evicts the stale copy
  cached on others instead of waiting for `localTTL` to expire.
- **`GetOrLoad` with singleflight.** Concurrent misses for the same key
  collapse into a single loader call. Everyone else waits and shares the
  result, instead of a thundering herd hitting your database.
- **Safe against slow loaders.** A loader that's still running when a
  newer `Set`, `Invalidate`, or another `GetOrLoad` for the same key
  completes won't clobber it with a stale result once it finally
  returns. Every key has a version counter redis itself owns and
  increments on every write, immune to clock skew between instances; a
  write that's no longer current is dropped instead of applied. See
  `GetOrLoad` below.
- **Generic, typed helpers.** `GetOrLoad[T]` and `WithCache` marshal/
  unmarshal a `T` for you (JSON by default, pluggable via `Marshaler`), so
  call sites stop hand-rolling `json.Marshal`/`Unmarshal` around every
  cache call.
- **Either tier is optional.** `WithoutRedis()` runs local-only (no redis
  dependency at all); `WithoutLocalCache()` runs redis-only (no per-instance
  memory, no staleness window).
- **Pluggable wire-format codec.** `Codec` transforms values before they hit
  redis and after they come back, e.g. compression, without touching the
  local tier, which always holds the plain decoded value.
- **Observability, not silence.** An `Observer` is notified on redis
  failures, codec failures, and background `Set` failures inside
  `GetOrLoad`, all of which fail open (never surfaced as an error to the
  caller) but are worth knowing about.
- **Built-in hit/miss/error counters.** `Stats()` returns a cumulative
  snapshot of local and redis hits/misses, redis, encode, decode, and set
  errors, and stale writes GetOrLoad dropped to protect against the race
  below, tracked with plain atomics on the request path, cheap enough to
  leave on always.
- **Pluggable latency histograms.** `WithLocalLatencyHistogram`,
  `WithRedisLatencyHistogram`, and `WithPubSubLatencyHistogram` take any
  type with an `Observe(seconds float64)` method, a prometheus histogram
  works as-is, and record how long each local tier lookup, redis data
  round trip (`Get`/`Set`/`Del`), and invalidation publish takes,
  respectively. Three separate histograms because all three have different
  scales and failure modes: local is a lock-free map read, redis data
  calls are a network round trip, and a publish is a fire-and-forget
  broadcast.
- **Small, composable interfaces.** `Cache` is built from `Reader`,
  `Writer`, `Loader`, and `Invalidator`. Depend on the smallest one your
  code actually needs, and mock accordingly.

## Install

```
go get github.com/otmaneki/strata
```

```go
import "github.com/otmaneki/strata"
```

## Usage

### Basic Get/Set

```go
tc := strata.NewTieredCache(redisClient, time.Minute, time.Hour)
defer tc.Close()

if err := tc.Set(ctx, "greeting", []byte("hello")); err != nil {
    log.Fatal(err)
}

val, ok := tc.Get(ctx, "greeting")
```

### Cache-aside with GetOrLoad

```go
data, err := tc.GetOrLoad(ctx, "user:1234", func(ctx context.Context) ([]byte, error) {
    u, err := db.GetUser(ctx, 1234)
    if err != nil {
        return nil, err
    }
    return json.Marshal(u)
})
```

Concurrent calls for the same missing key share one `db.GetUser` call, not
one per caller.

### Typed values, no manual marshaling

```go
user, err := strata.GetOrLoad(ctx, tc, strata.JSONMarshaler[User]{}, "user:1234",
    func(ctx context.Context) (User, error) {
        return db.GetUser(ctx, 1234)
    },
)
```

Or memoize a whole function once, at wiring time:

```go
getUser := strata.WithCache(tc, strata.JSONMarshaler[User]{},
    func(id string) string { return "user:" + id },
    func(ctx context.Context, id string) (User, error) { return db.GetUser(ctx, id) },
)

user, err := getUser(ctx, "1234")
```

### Cross-instance invalidation

```go
tc.SubscribeInvalidations(ctx) // once, at startup, on every instance

// later, on any instance:
tc.Invalidate(ctx, "user:1234") // every subscribed instance drops its local copy
```

### Local-only or redis-only

```go
// No redis dependency at all.
tc := strata.NewTieredCache(nil, time.Minute, 0, strata.WithoutRedis())

// No per-instance memory; every call hits redis.
tc := strata.NewTieredCache(redisClient, 0, time.Hour, strata.WithoutLocalCache())
```

### Observing failures and compressing over the wire

```go
tc := strata.NewTieredCache(redisClient, time.Minute, time.Hour,
    strata.WithObserver(myMetricsObserver),
    strata.WithCodec(myGzipCodec),
)
```

### Reading hit/miss/error counters

```go
stats := tc.Stats()
log.Printf("local hit rate: %d/%d", stats.LocalHits, stats.LocalHits+stats.LocalMisses)
```

`Stats()` is a cheap, cumulative snapshot. Call it on whatever interval
your metrics system scrapes on, no need to cache the result yourself.

### Latency histograms

```go
tc := strata.NewTieredCache(redisClient, time.Minute, time.Hour,
    strata.WithLocalLatencyHistogram(myLocalHistogram),
    strata.WithRedisLatencyHistogram(myRedisHistogram),
    strata.WithPubSubLatencyHistogram(myPubSubHistogram),
)
```

Bucket boundaries are up to you, and all three histograms will usually want
different ones given the scale and failure-mode differences above.

More runnable examples for every option live in `example_test.go`.

## How it works

- **`Get`** checks the local tier first. On a miss, it reads redis; a hit is
  decoded (if a `Codec` is set) and promoted into the local tier so the
  next `Get` for that key is served locally. A redis error that isn't a
  genuine miss is reported to the `Observer`, not the caller. `Get` fails
  open and looks like a miss either way, since that's the safer default for
  a cache.
- **`Set`** writes to the local tier immediately, then atomically writes
  to redis and bumps the key's version counter via a small Lua script
  (`setScript`), then publishes an invalidation, the same one `Invalidate`
  sends, so other instances drop their now-stale copy instead of serving
  it until `localTTL` expires. If a `Codec` is configured, only the redis
  copy is transformed, the local tier always holds the original value, so
  a warm local read never pays an encode/decode cost. The value redis
  stores under the key is always exactly what was given to `Set`, nothing
  prepended or wrapped around it; the version lives in a separate,
  `strata:ver:`-prefixed key redis owns, hash-tagged (`{key}`) so it
  shares a Cluster slot with the value key.
- **`Invalidate`** deletes locally, then atomically deletes the redis key
  and bumps its version counter (`delScript`), then publishes the key on
  a pub/sub channel. Every instance that called `SubscribeInvalidations`
  evicts that key from its own local tier when the message arrives. Every
  published message is tagged with the publishing instance's own id, so an
  instance that's subscribed to its own invalidations, the normal case when
  every instance both reads and writes, doesn't undo its own `Set`.
  Bumping the version on delete, not just removing the value, matters: a
  `GetOrLoad` loader already in flight for this key must see that
  something changed and refuse to resurrect it.
- **`GetOrLoad`** wraps `Get`, a `singleflight.Group`, and a version-aware
  write: on a miss, only one goroutine per key runs the loader; everyone
  else waits for it and shares the result. A loader can be slow, and if a
  `Set`, `Invalidate`, or another `GetOrLoad` for the same key completes
  while it's still running, writing the loader's now-stale result
  afterward would silently corrupt redis with nothing left to correct it.
  Before running the loader, `GetOrLoad` reads the key's current version;
  after the loader returns, a CAS script (`casScript`) writes the result
  only if that version is still current, otherwise it's dropped (counted
  in `Stats().StaleWritesDropped`) and whatever's already there is left
  alone. Because the version is a counter redis itself increments, not a
  timestamp either side computes locally, this is immune to clock skew
  between instances. The loader's result is still returned to its own
  caller either way, only the cache write is skipped. If caching the
  loaded value afterward fails outright (a redis error, not a staleness
  rejection), the loaded value is still returned, the loader already did
  the real work, so a redis write failure shouldn't fail the caller's
  request on top of it. The shared loader call runs on a context
  detached from any single caller's cancellation, bounded instead by
  `WithLoaderTimeout` (default 30s), so one caller giving up doesn't
  abort work the others sharing it still need. A panicking loader is
  recovered and returned as an error rather than crashing the process.
- **The local tier** is a `sync.Map` with an approximate size bound and a
  background TTL sweep, lock-free reads and best-effort eviction (no LRU
  ordering, since `sync.Map` doesn't track access order), documented as
  such rather than pretending to be exact.

### Copying semantics

`Set` always copies value into the local tier, so a buffer you reuse or
return to a pool (e.g. `sync.Pool`) right after calling `Set` is safe:
strata never retains it. `GetOrLoad` copies the `[]byte` its loader
returns before caching it, for the same reason, a loader reading into a
reused buffer is safe too. A redis-hit promotion inside `Get` also copies
before storing into the local tier, so mutating what that particular
`Get` call returned doesn't reach back into the cache either.

The one deliberate exception is a warm local-tier hit: `Get` returns the
local tier's own backing array directly, not a copy, because copying on
every local hit would undo most of the point of having a lock-free local
tier in the first place. Mutating that specific returned slice in place
corrupts the cached value for every other caller that hits the same key
afterward, silently, until the key is next overwritten or evicted. Treat
it as read-only, and copy it yourself if you need to mutate it:

```go
val, _ := tc.Get(ctx, "key") // may be a warm local hit
own := bytes.Clone(val)      // or append([]byte(nil), val...)
own[0] = 'x'                 // safe: own doesn't alias the cache
```

If you'd rather not think about which case you're in, the generic
`GetOrLoad[T]`/`WithCache` layer doesn't have this problem at all:
`Marshaler.Unmarshal` decodes into a fresh `T` on every call, so there's
nothing shared to corrupt.

## Benchmarks

Run with `make bench REDIS_ADDR=host:port` (`go test -bench . -benchmem`,
default `-benchtime`) against a real redis; there's no fake in-process
stand-in. That matters for these specific numbers: `Set`, `Invalidate`,
and `GetOrLoad`'s write path run a small Lua script in redis (see "How it
works" above), and a pure-Go Lua reimplementation would spin up a fresh
VM per call, at a few hundred allocations each, dwarfing everything else
being measured. Real redis runs that script server-side, where it costs
about what any other command does, and the numbers below reflect that.

To get your own redis instance and reproduce them:

```
docker run -d --rm --name strata-bench-redis -p 16379:6379 redis:7-alpine
make bench REDIS_ADDR=127.0.0.1:16379
docker stop strata-bench-redis
```

Any redis reachable from this machine works, not just a local container,
point `REDIS_ADDR` at it the same way. Run it a couple of times, `ns/op`
moves around with whatever else is on the box and the network path to
redis; the allocation counts shouldn't, they're deterministic given the
code, and are the numbers worth actually comparing against what's below.

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 PRO 4750U with Radeon Graphics

BenchmarkRedisOnly/Set-16                      32000    97041 ns/op    328 B/op     9 allocs/op
BenchmarkRedisOnly/Get-16                      17204   117757 ns/op    280 B/op     6 allocs/op
BenchmarkRedisOnly/GetParallel-16             184051    13222 ns/op    280 B/op     6 allocs/op

BenchmarkLocalOnly/Set-16                    3283226      795 ns/op    211 B/op     6 allocs/op
BenchmarkLocalOnly/Get-16                   39953786       59 ns/op      0 B/op     0 allocs/op
BenchmarkLocalOnly/GetParallel-16          344854586        7 ns/op      0 B/op     0 allocs/op

BenchmarkTieredCache/Set-16                    10000   245527 ns/op   1077 B/op    29 allocs/op
BenchmarkTieredCache/GetWarmLocal-16        36208272       67 ns/op      0 B/op     0 allocs/op
BenchmarkTieredCache/GetColdLocal-16           17598   118104 ns/op    519 B/op    12 allocs/op
BenchmarkTieredCache/GetWarmLocalParallel-16 137394918    18 ns/op      0 B/op     0 allocs/op
BenchmarkTieredCache/GetOrLoad-16           34781181       70 ns/op      0 B/op     0 allocs/op

BenchmarkTieredCache/GetOrLoadContended-16        568  4107398 ns/op  23234 B/op   970 allocs/op
                                                        1.000 loader-calls/round
BenchmarkTieredCache/GetOrLoadContendedNoDedup-16 459  5076820 ns/op  57950 B/op  1832 allocs/op
                                                       32.00 loader-calls/round

BenchmarkGetOrLoad_Generic/Set-16                4918   596327 ns/op   2563 B/op    97 allocs/op
BenchmarkGetOrLoad_Generic/GetWarmLocal-16    2304787      978 ns/op     80 B/op     2 allocs/op
BenchmarkGetOrLoad_Generic/GetWarmLocalParallel-16 18019952 128 ns/op   80 B/op     2 allocs/op
BenchmarkWithCache-16                         1672387     1376 ns/op    152 B/op     4 allocs/op
```

**What these say:**

- **A warm local read is ~67ns in `TieredCache`, ~59ns in `LocalOnly`.**
  Tiering costs almost nothing once a key is promoted. Compare that to
  `~118µs` for a plain redis `Get` over a real connection: roughly
  **1,750x** faster.
- **`GetColdLocal` (118µs) tracks the raw redis `Get` (118µs) exactly.**
  The local-miss check is cheap, so falling back to redis costs exactly
  one round trip, no hidden tiering tax.
- **Parallel redis reads drop to ~13µs.** Connection-pool overlap
  amortizing round-trip latency across goroutines, not redis getting
  faster per call. `LocalOnly`'s parallel warm read drops much further,
  to ~7ns, `sync.Map`'s lock-free read path doing what it's for.
  `TieredCache`'s parallel warm read is ~18ns, roughly 2.5x `LocalOnly`'s:
  the difference is `Stats`'s atomic counters contending with each other
  under 16-way parallelism, a measured, accepted cost of leaving
  hit/miss/error counting on by default, not a free lunch.
- **`Set` (245µs, 29 allocs) costs about 2.5x a plain redis `Set` (97µs,
  9 allocs).** That's the version-bumping Lua script plus the
  invalidation publish, both real redis round trips on top of the local
  write, not free, but nowhere near what a pure-Go Lua reimplementation
  would cost, see the intro above.
- **`GetOrLoadContended` proves the singleflight dedup**: 32 goroutines
  racing a cold key produce exactly **1.000 loader calls per round**,
  versus **32.00** without it (`GetOrLoadContendedNoDedup`), and roughly
  half the allocations per round too (970 vs 1832). That's the
  thundering-herd protection actually measured, not just asserted.
- **The generic helpers cost what JSON marshaling costs**, not what the
  cache costs, on the warm path: `GetOrLoad[T]`'s is ~1µs and
  `WithCache`'s is ~1.4µs, both dominated by `encoding/json`, not the
  ~60ns cache lookup underneath them. `GetOrLoad_Generic/Set` (~596µs,
  always a cold key) is dominated by the same cold-write round trips as
  `TieredCache/Set` above, plus marshaling.

## Development

```
make build   # go build ./...
make test    # go test -v -race -count=1 ./...
make vet     # go vet ./...
make fmt     # gofmt -l .
make bench   # all benchmarks
```

`test` and every `bench*` target except `bench-local` need a real redis
reachable at `REDIS_ADDR=host:port`, e.g.
`make test REDIS_ADDR=localhost:6379`; nothing fakes one for you. CI runs
the test suite against a `redis:7-alpine` service container, see
`.github/workflows/go.yml`.

Linting uses [golangci-lint](https://golangci-lint.run) with the config in
`.golangci.yaml`:

```
golangci-lint run ./...
```
