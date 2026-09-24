package strata

import "sync/atomic"

// Stats is a snapshot of a TieredCache's hit/miss/error counters. It's a
// point-in-time copy, not a live view. Call TieredCache.Stats again for
// updated numbers. Counters are cumulative for the life of the TieredCache
// and never reset; scrape periodically and let your metrics system compute
// rates and deltas, the way Prometheus-style counters are meant to be read.
//
// Latency isn't tracked here. Counting is cheap with a plain atomic
// increment per event, but a meaningful latency measurement needs a real
// histogram, which is a job for whatever metrics library you're already
// using, not something to reimplement here. Wire it up via Observer
// instead once that's in place.
type Stats struct {
	// LocalHits and LocalMisses count local-tier lookups in Get. Both stay
	// zero if WithoutLocalCache is used, since there's no local tier to
	// check.
	LocalHits   uint64
	LocalMisses uint64

	// RedisHits, RedisMisses, and RedisErrors count redis Get outcomes: a
	// value found, redis.Nil (a genuine miss), and any other error,
	// respectively. All three stay zero if WithoutRedis is used.
	RedisHits   uint64
	RedisMisses uint64
	RedisErrors uint64

	// EncodeErrors and DecodeErrors count Codec failures in Set and Get.
	// Both stay zero if no Codec is configured.
	EncodeErrors uint64
	DecodeErrors uint64

	// SetErrors counts GetOrLoad's internal Set failing after a successful
	// loader call. See Observer.OnSetError's doc comment for why that
	// fails open instead of returning the error to GetOrLoad's caller.
	SetErrors uint64
}

// tieredStats holds Stats's counters as atomics, incremented inline on
// TieredCache's request path. Kept as a separate, unexported type so
// TieredCache's own fields aren't cluttered with the atomics themselves,
// callers only ever see the plain-value Stats snapshot.
type tieredStats struct {
	localHits, localMisses              atomic.Int64
	redisHits, redisMisses, redisErrors atomic.Int64
	encodeErrors, decodeErrors          atomic.Int64
	setErrors                           atomic.Int64
}

func (s *tieredStats) snapshot() Stats {
	return Stats{
		LocalHits:    uint64(s.localHits.Load()),
		LocalMisses:  uint64(s.localMisses.Load()),
		RedisHits:    uint64(s.redisHits.Load()),
		RedisMisses:  uint64(s.redisMisses.Load()),
		RedisErrors:  uint64(s.redisErrors.Load()),
		EncodeErrors: uint64(s.encodeErrors.Load()),
		DecodeErrors: uint64(s.decodeErrors.Load()),
		SetErrors:    uint64(s.setErrors.Load()),
	}
}
