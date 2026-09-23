# strata

A two-tier cache for Go: an in-process local cache sits in front of redis,
with pub/sub-driven invalidation so a write on one node evicts the stale
copy cached on others.

The name comes from geology — strata are the layers of rock that build up
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
fallback and the source of truth other instances agree on, and an
`Invalidate` call on one instance propagates to the local cache of every
other instance subscribed to it. You get the speed of a local cache with
the consistency guarantees of a shared one, without hand-rolling the
promotion/invalidation logic yourself.

## Features

- **Two tiers, one API.** `Get`/`Set` transparently check local first, fall
  back to redis, and promote redis hits into the local tier.
- **Cross-instance invalidation.** `Invalidate` removes a key everywhere and
  publishes the eviction over redis pub/sub; `SubscribeInvalidations` picks
  those up on every other instance.
- **`GetOrLoad` with singleflight.** Concurrent misses for the same key
  collapse into a single loader call — everyone else waits and shares the
  result, instead of a thundering herd hitting your database.
- **Generic, typed helpers.** `GetOrLoad[T]` and `WithCache` marshal/
  unmarshal a `T` for you (JSON by default, pluggable via `Marshaler`), so
  call sites stop hand-rolling `json.Marshal`/`Unmarshal` around every
  cache call.
- **Either tier is optional.** `WithoutRedis()` runs local-only (no redis
  dependency at all); `WithoutLocalCache()` runs redis-only (no per-instance
  memory, no staleness window).
- **Pluggable wire-format codec.** `Codec` transforms values before they hit
  redis and after they come back — e.g. compression — without touching the
  local tier, which always holds the plain decoded value.
- **Observability, not silence.** An `Observer` is notified on redis
  failures, codec failures, and background `Set` failures inside
  `GetOrLoad` — all of which fail open (never surfaced as an error to the
  caller) but are worth knowing about.
- **Small, composable interfaces.** `Cache` is built from `Reader`,
  `Writer`, `Loader`, and `Invalidator` — depend on the smallest one your
  code actually needs, and mock accordingly.

## Install

The module is currently named `strata` with no host prefix, which only
resolves for code within this same module. Once this repo is pushed
somewhere `go get` can reach (e.g. `github.com/<you>/strata`), update
`go.mod`'s module line and the import path below to match.

```
go get strata
```

```go
import "strata"
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

More runnable examples for every option live in `example_test.go`.

## How it works

- **`Get`** checks the local tier first. On a miss, it reads redis; a hit is
  decoded (if a `Codec` is set) and promoted into the local tier so the
  next `Get` for that key is served locally. A redis error that isn't a
  genuine miss is reported to the `Observer`, not the caller — `Get` fails
  open and looks like a miss either way, since that's the safer default for
  a cache.
- **`Set`** writes to the local tier immediately and to redis. If a `Codec`
  is configured, only the redis copy is transformed — the local tier always
  holds the original value, so a warm local read never pays an encode/
  decode cost.
- **`Invalidate`** deletes locally, deletes in redis, then publishes the key
  on a pub/sub channel. Every instance that called `SubscribeInvalidations`
  evicts that key from its own local tier when the message arrives.
- **`GetOrLoad`** wraps `Get`, a `singleflight.Group`, and `Set`: on a miss,
  only one goroutine per key runs the loader; everyone else waits for it
  and shares the result. If caching the loaded value afterward fails, the
  loaded value is still returned — the loader already did the real work,
  so a redis write failure shouldn't fail the caller's request on top of
  it.
- **The local tier** is a `sync.Map` with an approximate size bound and a
  background TTL sweep — lock-free reads, best-effort eviction (no LRU
  ordering, since `sync.Map` doesn't track access order), documented as
  such rather than pretending to be exact.

## Benchmarks

Run with `make bench` (`go test -bench . -benchmem`, default `-benchtime`),
against an in-process miniredis instance — no real network hop, so these
are a lower bound on how much redis actually costs relative to the local
tier; a real network round trip only widens the gap. Point `REDIS_ADDR` at
a real redis instance for numbers that include one (`make bench
REDIS_ADDR=host:port`).

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 PRO 4750U with Radeon Graphics

BenchmarkRedisOnly/Set-16                   19516    65932 ns/op    1241 B/op    34 allocs/op
BenchmarkRedisOnly/Get-16                   18526    66044 ns/op     456 B/op    18 allocs/op
BenchmarkRedisOnly/GetParallel-16          307263     3617 ns/op     472 B/op    18 allocs/op

BenchmarkLocalOnly/Set-16                 1769337      808 ns/op     223 B/op     6 allocs/op
BenchmarkLocalOnly/Get-16                20308600       60 ns/op       0 B/op     0 allocs/op
BenchmarkLocalOnly/GetParallel-16       170281178        7 ns/op       0 B/op     0 allocs/op

BenchmarkTieredCache/Set-16                 23566    79940 ns/op    1637 B/op    38 allocs/op
BenchmarkTieredCache/GetWarmLocal-16     19354909       61 ns/op       0 B/op     0 allocs/op
BenchmarkTieredCache/GetColdLocal-16        13897    79409 ns/op     628 B/op    23 allocs/op
BenchmarkTieredCache/GetWarmLocalParallel-16 167177767   7 ns/op       0 B/op     0 allocs/op
BenchmarkTieredCache/GetOrLoad-16        18399337       65 ns/op       0 B/op     0 allocs/op

BenchmarkTieredCache/GetOrLoadContended-16          391  3034935 ns/op   32413 B/op  1348 allocs/op
                                                          1.000 loader-calls/round
BenchmarkTieredCache/GetOrLoadContendedNoDedup-16   338  3438065 ns/op   69631 B/op  2454 allocs/op
                                                         31.93 loader-calls/round

BenchmarkGetOrLoad_Generic/GetWarmLocal-16        1050206     1295 ns/op    80 B/op   2 allocs/op
BenchmarkWithCache-16                             1000000     1650 ns/op   152 B/op   4 allocs/op
```

**What these say:**

- **A warm local read is ~61ns, whether it's `LocalOnly` or `TieredCache`** —
  tiering costs nothing once a key is promoted. Compare that to `~66µs` for
  a plain redis `Get`: roughly **1,000x** faster, even against miniredis
  with no real network involved.
- **`GetColdLocal` (79µs) tracks the raw redis `Get` (66µs) closely** — the
  local-miss check is cheap, so falling back to redis costs exactly one
  round trip, no hidden tiering tax.
- **Parallel redis reads drop to ~3.6µs** — connection-pool overlap
  amortizing round-trip latency across goroutines, not redis getting
  faster per call. `LocalOnly`/`TieredCache` warm reads scale the same way
  down to ~7ns, `sync.Map`'s lock-free read path doing what it's for.
- **`GetOrLoadContended` proves the singleflight dedup**: 32 goroutines
  racing a cold key produce exactly **1.000 loader calls per round**,
  versus **31.93** without it (`GetOrLoadContendedNoDedup`) — that's the
  thundering-herd protection actually measured, not just asserted, and it
  roughly halves allocations per round too (32KB vs 70KB).
- **The generic helpers cost what JSON marshaling costs**, not what the
  cache costs — `GetOrLoad[T]`'s warm path is ~1.3µs and `WithCache`'s is
  ~1.65µs, both dominated by `encoding/json`, not the ~60ns cache lookup
  underneath them.

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
