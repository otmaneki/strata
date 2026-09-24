package strata

import (
	"context"
	"fmt"
	"testing"
)

// benchUser simulates a realistic cached value for the generic layer, the
// struct form of what benchValue (in cache_bench_test.go) already
// represents as raw JSON, so these benchmarks measure the actual
// Marshal/Unmarshal cost GetOrLoad[T] adds on top of the already-benchmarked
// byte-level path in BenchmarkTieredCache.
var benchUser = testUser{ID: "1234", Name: "otmane"}

func BenchmarkGetOrLoad_Generic(b *testing.B) {
	ctx := context.Background()

	newCache := func(b *testing.B) *TieredCache {
		tc := NewTieredCache(newBenchRedisClient(b), benchTTL, benchTTL)
		b.Cleanup(func() { _ = tc.Close() })
		return tc
	}

	loader := func(context.Context) (testUser, error) {
		return benchUser, nil
	}

	b.Run("Set", func(b *testing.B) {
		tc := newCache(b)
		b.ReportAllocs()
		for i := range b.N {
			key := fmt.Sprintf("bench:generic:set:%d", i)
			if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader); err != nil {
				b.Fatal(err)
			}
		}
	})

	// GetWarmLocal is the steady-state hot path: value already promoted
	// into the local tier, so this isolates Unmarshal's cost on top of a
	// lock-free local read. Compare against BenchmarkTieredCache/GetWarmLocal
	// to see the marshaling overhead specifically.
	b.Run("GetWarmLocal", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:generic:get-warm"
		if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		for range b.N {
			if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetWarmLocalParallel", func(b *testing.B) {
		tc := newCache(b)
		const key = "bench:generic:get-warm-parallel"
		if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkWithCache benchmarks the WithCache decorator, which is just
// GetOrLoad curried behind a keyFn, mainly confirms currying itself doesn't
// add measurable overhead on top of BenchmarkGetOrLoad_Generic's numbers.
func BenchmarkWithCache(b *testing.B) {
	ctx := context.Background()
	tc := NewTieredCache(newBenchRedisClient(b), benchTTL, benchTTL)
	b.Cleanup(func() { _ = tc.Close() })

	getUser := WithCache(
		tc, JSONMarshaler[testUser]{},
		func(id string) string { return "bench:withcache:" + id },
		func(context.Context, string) (testUser, error) {
			return benchUser, nil
		},
	)

	if _, err := getUser(ctx, "warm"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for range b.N {
		if _, err := getUser(ctx, "warm"); err != nil {
			b.Fatal(err)
		}
	}
}
