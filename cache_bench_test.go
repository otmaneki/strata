package strata

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// benchValue simulates a realistic cached payload (e.g. a serialized JSON row).
var benchValue = []byte(`{"id":1234,"name":"otmane","email":"otmane@example.com","active":true}`)

const benchTTL = time.Minute

// newBenchRedisClient returns a *redis.Client to benchmark against.
//
// By default it spins up an in-process miniredis instance, so `go test -bench`
// works with no external services. Point REDIS_ADDR at a real redis instance
// (e.g. `REDIS_ADDR=localhost:6379 go test -bench .`) to get numbers that
// include real network/IO overhead instead of a loopback fake.
func newBenchRedisClient(b *testing.B) *redis.Client {
	b.Helper()

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{Addr: addr})
		if err := client.Ping(context.Background()).Err(); err != nil {
			b.Fatalf("could not reach REDIS_ADDR=%s: %v", addr, err)
		}
		b.Cleanup(func() { _ = client.Close() })
		return client
	}

	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("could not start miniredis: %v", err)
	}
	b.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	return client
}

// BenchmarkRedisOnly benchmarks talking to redis directly, with no local tier.
func BenchmarkRedisOnly(b *testing.B) {
	ctx := context.Background()
	client := newBenchRedisClient(b)

	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			key := fmt.Sprintf("bench:redis:set:%d", i)
			if err := client.Set(ctx, key, benchValue, benchTTL).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get", func(b *testing.B) {
		const key = "bench:redis:get"
		if err := client.Set(ctx, key, benchValue, benchTTL).Err(); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		for range b.N {
			if _, err := client.Get(ctx, key).Bytes(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetParallel", func(b *testing.B) {
		const key = "bench:redis:get-parallel"
		if err := client.Set(ctx, key, benchValue, benchTTL).Err(); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := client.Get(ctx, key).Bytes(); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkLocalOnly benchmarks the in-process localCache tier in isolation.
func BenchmarkLocalOnly(b *testing.B) {
	newCache := func(b *testing.B) *localCache {
		c := newLocalCache(1_000_000, time.Hour)
		b.Cleanup(c.Close)
		return c
	}

	b.Run("Set", func(b *testing.B) {
		c := newCache(b)
		b.ReportAllocs()
		for i := range b.N {
			key := fmt.Sprintf("bench:local:set:%d", i)
			c.Set(key, benchValue, benchTTL)
		}
	})

	b.Run("Get", func(b *testing.B) {
		c := newCache(b)
		const key = "bench:local:get"
		c.Set(key, benchValue, benchTTL)

		b.ReportAllocs()
		for range b.N {
			if _, ok := c.Get(key); !ok {
				b.Fatal("expected cache hit")
			}
		}
	})

	b.Run("GetParallel", func(b *testing.B) {
		c := newCache(b)
		const key = "bench:local:get-parallel"
		c.Set(key, benchValue, benchTTL)

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok := c.Get(key); !ok {
					b.Fatal("expected cache hit")
				}
			}
		})
	})
}

// BenchmarkTieredCache benchmarks the full local+redis tiered cache.
func BenchmarkTieredCache(b *testing.B) {
	ctx := context.Background()

	newCache := func(b *testing.B) *TieredCache {
		tc := NewTieredCache(newBenchRedisClient(b), benchTTL, benchTTL)
		b.Cleanup(func() { _ = tc.Close() })
		return tc
	}

	b.Run("Set", func(b *testing.B) {
		tc := newCache(b)
		b.ReportAllocs()
		for i := range b.N {
			key := fmt.Sprintf("bench:tiered:set:%d", i)
			if err := tc.Set(ctx, key, benchValue); err != nil {
				b.Fatal(err)
			}
		}
	})

	// GetWarmLocal is the steady-state hot path: value already promoted into
	// the local tier, so no redis round trip is needed.
	b.Run("GetWarmLocal", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:get-warm"
		if err := tc.Set(ctx, key, benchValue); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		for range b.N {
			if _, ok := tc.Get(ctx, key); !ok {
				b.Fatal("expected cache hit")
			}
		}
	})

	// GetColdLocal simulates a local-tier miss (e.g. right after eviction, or
	// on a different process instance) that falls through to redis.
	b.Run("GetColdLocal", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:get-cold"
		if err := tc.redis.Set(ctx, key, benchValue, benchTTL).Err(); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		for range b.N {
			tc.local.Delete(key)
			if _, ok := tc.Get(ctx, key); !ok {
				b.Fatal("expected cache hit")
			}
		}
	})

	b.Run("GetWarmLocalParallel", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:get-warm-parallel"
		if err := tc.Set(ctx, key, benchValue); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok := tc.Get(ctx, key); !ok {
					b.Fatal("expected cache hit")
				}
			}
		})
	})

	b.Run("GetOrLoad", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:getorload"
		loader := func(ctx context.Context) ([]byte, error) {
			return benchValue, nil
		}

		b.ReportAllocs()
		for range b.N {
			if _, err := tc.GetOrLoad(ctx, key, loader); err != nil {
				b.Fatal(err)
			}
		}
	})

	// GetOrLoadContended measures singleflight's dedup benefit: each round,
	// the key is invalidated (both tiers) and `concurrency` goroutines call
	// GetOrLoad on it simultaneously while it's cold. Without singleflight
	// this would be a thundering herd of `concurrency` loader calls; with it,
	// only the first caller should actually invoke the loader and everyone
	// else waits and shares the result.
	b.Run("GetOrLoadContended", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:getorload-contended"
		const concurrency = 32
		const loadLatency = 2 * time.Millisecond

		var loaderCalls int64
		loader := func(_ context.Context) ([]byte, error) { //nolint:unparam // this is just a test file
			atomic.AddInt64(&loaderCalls, 1)
			time.Sleep(loadLatency) // simulate a slow origin fetch (DB/API call)
			return benchValue, nil
		}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			b.StopTimer()
			if err := tc.Invalidate(ctx, key); err != nil {
				b.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(concurrency)
			b.StartTimer()

			for range concurrency {
				go func() {
					defer wg.Done()
					if _, err := tc.GetOrLoad(ctx, key, loader); err != nil {
						b.Error(err)
					}
				}()
			}
			wg.Wait()
		}
		b.StopTimer()

		b.ReportMetric(float64(loaderCalls)/float64(b.N), "loader-calls/round")
	})

	// GetOrLoadContendedNoDedup is the same thundering-herd scenario but
	// bypasses singleflight (plain Get-miss-load-Set), to show what
	// GetOrLoadContended's numbers would look like without the dedup.
	b.Run("GetOrLoadContendedNoDedup", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:tiered:getorload-contended-nodedup"
		const concurrency = 32
		const loadLatency = 2 * time.Millisecond

		var loaderCalls int64
		naiveGetOrLoad := func(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
			if val, ok := tc.Get(ctx, key); ok {
				return val, nil
			}
			val, err := loader(ctx)
			if err != nil {
				return nil, err
			}
			return val, tc.Set(ctx, key, val)
		}
		loader := func(_ context.Context) ([]byte, error) { //nolint:unparam // this is just a test file
			atomic.AddInt64(&loaderCalls, 1)
			time.Sleep(loadLatency)
			return benchValue, nil
		}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			b.StopTimer()
			if err := tc.Invalidate(ctx, key); err != nil {
				b.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(concurrency)
			b.StartTimer()

			for range concurrency {
				go func() {
					defer wg.Done()
					if _, err := naiveGetOrLoad(ctx, key, loader); err != nil {
						b.Error(err)
					}
				}()
			}
			wg.Wait()
		}
		b.StopTimer()

		b.ReportMetric(float64(loaderCalls)/float64(b.N), "loader-calls/round")
	})
}
