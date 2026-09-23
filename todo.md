# TODOs for strata, the 2 tier-caching library.

- [ ] Add batching support for all the commands
- [ ] Add batch loader as well
- [ ] Add metrics for the in-mem cache hits/misses size etc.. and the latency for it, as well as for the remote. use atomics for this.
- [x] Add `WithCache` function that takes in a callback and caches the actual result of that function
- [x] Make it more configurable with the options pattern
- [x] Add some examples in how to use this.
- [x] Add custom marshallers or encoders to the SET and GET and the batching too

