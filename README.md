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
  newer `Set` for the same key completes won't clobber it with a stale
  result once it finally returns, every redis write is versioned and a
  stale one is dropped instead of applied. See `GetOrLoad` below.
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
- **`Set`** writes to the local tier immediately and to redis, then
  publishes an invalidation, the same one `Invalidate` sends, so other
  instances drop their now-stale copy instead of serving it until
  `localTTL` expires. If a `Codec` is configured, only the redis copy is
  transformed, the local tier always holds the original value, so a warm
  local read never pays an encode/decode cost.
- **`Invalidate`** deletes locally, deletes in redis, then publishes the key
  on a pub/sub channel. Every instance that called `SubscribeInvalidations`
  evicts that key from its own local tier when the message arrives. Every
  published message is tagged with the publishing instance's own id, so an
  instance that's subscribed to its own invalidations, the normal case when
  every instance both reads and writes, doesn't undo its own `Set`.
- **`GetOrLoad`** wraps `Get`, a `singleflight.Group`, and a version-aware
  write: on a miss, only one goroutine per key runs the loader; everyone
  else waits for it and shares the result. A loader can be slow, and if a
  plain `Set` for the same key lands in redis while it's still running,
  writing the loader's now-stale result afterward would silently corrupt
  redis with nothing left to correct it. Every redis write is tagged with
  a version, the time the write was decided, so GetOrLoad's write only
  applies if it's still newer than whatever's already stored; otherwise
  it's dropped (counted in `Stats().StaleWritesDropped`) and the fresher
  value is left alone. The loader's result is still returned to its own
  caller either way, only the cache write is skipped. If caching the
  loaded value afterward fails outright (a redis error, not a staleness
  rejection), the loaded value is still returned, the loader already did
  the real work, so a redis write failure shouldn't fail the caller's
  request on top of it.
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

Run with `make bench` (`go test -bench . -benchmem`, default `-benchtime`),
against an in-process miniredis instance, no real network hop, so these
are a lower bound on how much redis actually costs relative to the local
tier; a real network round trip only widens the gap. Point `REDIS_ADDR` at
a real redis instance for numbers that include one (`make bench
REDIS_ADDR=host:port`).

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 PRO 4750U with Radeon Graphics

BenchmarkRedisOnly/Set-16                       33836    73873 ns/op    1464 B/op    34 allocs/op
BenchmarkRedisOnly/Get-16                       36134    67178 ns/op     456 B/op    18 allocs/op
BenchmarkRedisOnly/GetParallel-16               573189     3523 ns/op     472 B/op    18 allocs/op

BenchmarkLocalOnly/Set-16                      3343795      833 ns/op     211 B/op     6 allocs/op
BenchmarkLocalOnly/Get-16                     39536169       61 ns/op       0 B/op     0 allocs/op
BenchmarkLocalOnly/GetParallel-16            316830915        8 ns/op       0 B/op     0 allocs/op

BenchmarkTieredCache/Set-16                      16848   152709 ns/op    2677 B/op    63 allocs/op
BenchmarkTieredCache/GetWarmLocal-16          35956658       66 ns/op       0 B/op     0 allocs/op
BenchmarkTieredCache/GetColdLocal-16             33204    69898 ns/op     685 B/op    24 allocs/op
BenchmarkTieredCache/GetWarmLocalParallel-16 141168278       17 ns/op       0 B/op     0 allocs/op
BenchmarkTieredCache/GetOrLoad-16             34539559       68 ns/op       0 B/op     0 allocs/op

BenchmarkTieredCache/GetOrLoadContended-16         669  3646355 ns/op  227642 B/op  2107 allocs/op
                                                         1.000 loader-calls/round
BenchmarkTieredCache/GetOrLoadContendedNoDedup-16  650  3539056 ns/op   94317 B/op  3242 allocs/op
                                                        31.96 loader-calls/round

BenchmarkGetOrLoad_Generic/Set-16                 3838   731769 ns/op  202976 B/op   892 allocs/op
BenchmarkGetOrLoad_Generic/GetWarmLocal-16     1898338     1198 ns/op      80 B/op     2 allocs/op
BenchmarkGetOrLoad_Generic/GetWarmLocalParallel-16 19671957 125 ns/op     80 B/op     2 allocs/op
BenchmarkWithCache-16                           1393260     1768 ns/op     152 B/op     4 allocs/op
```

**What these say:**

- **A warm local read is ~66ns in `TieredCache`, ~61ns in `LocalOnly`.**
  Tiering costs almost nothing once a key is promoted. Compare that to
  `~67µs` for a plain redis `Get`: roughly **1,000x** faster, even against
  miniredis with no real network involved.
- **`GetColdLocal` (70µs) tracks the raw redis `Get` (67µs) closely.** The
  local-miss check is cheap, so falling back to redis costs exactly one
  round trip, no hidden tiering tax.
- **Parallel redis reads drop to ~3.5µs.** Connection-pool overlap
  amortizing round-trip latency across goroutines, not redis getting
  faster per call. `LocalOnly`'s parallel warm read drops further, to
  ~8ns, `sync.Map`'s lock-free read path doing what it's for.
  `TieredCache`'s parallel warm read is ~17ns, roughly 2x `LocalOnly`'s:
  the difference is `Stats`'s atomic counters contending with each other
  under 16-way parallelism, a measured, accepted cost of leaving
  hit/miss/error counting on by default, not a free lunch.
- **`GetOrLoadContended` proves the singleflight dedup**: 32 goroutines
  racing a cold key produce exactly **1.000 loader calls per round**,
  versus **31.96** without it (`GetOrLoadContendedNoDedup`). That's the
  thundering-herd protection actually measured, not just asserted.
  `GetOrLoadContended`'s own allocations look heavy in this table (a
  single real write per round costs more than the 32 reads around it),
  that's miniredis's pure-Go Lua interpreter spinning up a VM for
  `setIfNewer`'s CAS script, not something a real redis server would cost
  a client: there, script execution happens server-side, and the
  client-side cost is comparable to any other command.
- **The generic helpers cost what JSON marshaling costs**, not what the
  cache costs, on the warm path: `GetOrLoad[T]`'s is ~1.2µs and
  `WithCache`'s is ~1.77µs, both dominated by `encoding/json`, not the
  ~60ns cache lookup underneath them. `GetOrLoad_Generic/Set` (~730µs,
  always a cold key) additionally pays the same miniredis Lua cost as
  `GetOrLoadContended` above, on top of marshaling.

## Development

```
make build   # go build ./...
make test    # go test -v -count=1 ./...
make vet     # go vet ./...
make fmt     # gofmt -l .
make bench   # all benchmarks, against miniredis by default
```

Linting uses [golangci-lint](https://golangci-lint.run) with the config in
`.golangci.yaml`:

```
golangci-lint run ./...
```
