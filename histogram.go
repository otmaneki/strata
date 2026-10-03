package strata

// Histogram records latency observations, in seconds. It matches the
// Observe method a prometheus client_golang Histogram already implements,
// so one can be passed in directly, no adapter needed. Any other metrics
// library's histogram works too, as long as it exposes the same method.
//
// Bucket boundaries are left entirely up to you, and the two histograms
// below will usually want different ones: a local tier lookup is a
// lock-free map read, typically tens of nanoseconds, while a redis round
// trip is a network call, typically hundreds of microseconds to a few
// milliseconds. Reusing one bucket layout for both will leave one of them
// with no useful resolution.
type Histogram interface {
	Observe(seconds float64)
}
