package strata

import "time"

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

// startLocalTimer/observeLocalLatency, and their redis/pubsub
// counterparts below, skip the clock read entirely when no Histogram is
// registered, keeping the With*LatencyHistogram options free for callers
// who don't use them, notably on the hot local-read path.
func (tc *TieredCache) startLocalTimer() time.Time {
	if tc.localLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observeLocalLatency(start time.Time) {
	if tc.localLatency != nil {
		tc.localLatency.Observe(time.Since(start).Seconds())
	}
}

func (tc *TieredCache) startRedisTimer() time.Time {
	if tc.redisLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observeRedisLatency(start time.Time) {
	if tc.redisLatency != nil {
		tc.redisLatency.Observe(time.Since(start).Seconds())
	}
}

func (tc *TieredCache) startPubSubTimer() time.Time {
	if tc.pubsubLatency == nil {
		return time.Time{}
	}
	return time.Now()
}

func (tc *TieredCache) observePubSubLatency(start time.Time) {
	if tc.pubsubLatency != nil {
		tc.pubsubLatency.Observe(time.Since(start).Seconds())
	}
}
