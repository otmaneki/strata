# TODOs for strata, the 2 tier-caching library.

- [x] Add metrics for the in-mem cache hits/misses etc.. as well as for the remote. use atomics for this. (`Stats()`)
- [x] Add `WithCache` function that takes in a callback and caches the actual result of that function
- [x] Make it more configurable with the options pattern
- [x] Add some examples in how to use this.
- [x] Add custom marshallers or encoders to the SET and GET and the batching too
- [x] Add latency metrics, via a pluggable `Histogram` interface for the redis and local tiers
- [ ] Add batch loader as well
- [ ] Add batching support for all the commands
