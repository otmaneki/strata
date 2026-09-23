package strata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testUser struct {
	ID   string
	Name string
}

func TestGetOrLoad_Generic_RoundTrip(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()

	var loaderCalls int64
	loader := func(context.Context) (testUser, error) {
		atomic.AddInt64(&loaderCalls, 1)
		return testUser{ID: "1234", Name: "otmane"}, nil
	}

	first, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1234", loader)
	if err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}
	if first != (testUser{ID: "1234", Name: "otmane"}) {
		t.Fatalf("first GetOrLoad = %+v, want the loaded user", first)
	}

	second, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1234", loader)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if second != first {
		t.Fatalf("second GetOrLoad = %+v, want %+v", second, first)
	}
	if calls := atomic.LoadInt64(&loaderCalls); calls != 1 {
		t.Fatalf("loader called %d times, want 1 (second call should be served from cache)", calls)
	}
}

func TestGetOrLoad_Generic_DedupsConcurrentMisses(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()

	var loaderCalls int64
	loader := func(context.Context) (testUser, error) { //nolint:unparam // this is just a test file
		atomic.AddInt64(&loaderCalls, 1)
		time.Sleep(20 * time.Millisecond) // simulate a slow origin fetch
		return testUser{ID: "1234", Name: "otmane"}, nil
	}

	const concurrency = 16
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			if _, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1234", loader); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if calls := atomic.LoadInt64(&loaderCalls); calls != 1 {
		t.Fatalf("loader called %d times across %d concurrent misses, want 1 (singleflight dedup should still apply)", calls, concurrency)
	}
}

func TestGetOrLoad_Generic_LoaderErrorPropagates(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()

	boom := errors.New("boom")
	loader := func(context.Context) (testUser, error) { return testUser{}, boom }

	_, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1234", loader)
	if err == nil {
		t.Fatal("expected the loader's error to propagate")
	}
}

func TestGetOrLoad_Generic_IncompatibleCachedValueIsAnError(t *testing.T) {
	tc, client := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "user:1234"

	// Seed the cache directly with bytes that aren't valid JSON for
	// testUser, simulating e.g. a different type sharing this key by
	// mistake, or a format change between deployments.
	if err := client.Set(ctx, key, []byte("not-json"), time.Minute).Err(); err != nil {
		t.Fatalf("seeding redis: %v", err)
	}

	loaderCalled := false
	loader := func(context.Context) (testUser, error) {
		loaderCalled = true
		return testUser{ID: "1234", Name: "otmane"}, nil
	}

	_, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, key, loader)
	if err == nil {
		t.Fatal("expected an unmarshal error, got nil")
	}
	if loaderCalled {
		t.Fatal("expected the loader NOT to be called: this is a genuine hit, just with incompatible bytes")
	}
}

func TestWithCache_MemoizesPerArgument(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()

	var loaderCalls int64
	getUser := WithCache(
		tc, JSONMarshaler[testUser]{},
		func(id string) string { return "user:" + id },
		func(_ context.Context, id string) (testUser, error) {
			atomic.AddInt64(&loaderCalls, 1)
			return testUser{ID: id, Name: "user-" + id}, nil
		},
	)

	first, err := getUser(ctx, "1")
	if err != nil {
		t.Fatalf("getUser(1) first call: %v", err)
	}
	if first.ID != "1" {
		t.Fatalf("getUser(1) = %+v, want ID=1", first)
	}

	if _, err := getUser(ctx, "1"); err != nil {
		t.Fatalf("getUser(1) second call: %v", err)
	}
	if calls := atomic.LoadInt64(&loaderCalls); calls != 1 {
		t.Fatalf("fn called %d times for id=1, want 1 (second call should be cached)", calls)
	}

	second, err := getUser(ctx, "2")
	if err != nil {
		t.Fatalf("getUser(2): %v", err)
	}
	if second.ID != "2" {
		t.Fatalf("getUser(2) = %+v, want ID=2", second)
	}
	if calls := atomic.LoadInt64(&loaderCalls); calls != 2 {
		t.Fatalf("fn called %d times total, want 2 (a different argument is a different cache entry)", calls)
	}
}

// The generic layer never touches tier logic directly — it only calls
// Cache.GetOrLoad — so these two are less "does the generic layer support
// tier bypass" and more "is that delegation actually as complete as it
// looks," the same distinction the byte-level WithoutLocalCache/WithoutRedis
// tests exist to prove rather than assume.

func TestGetOrLoad_Generic_ComposesWithWithoutLocalCache(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t, WithoutLocalCache())
	ctx := context.Background()

	var loaderCalls int64
	loader := func(context.Context) (testUser, error) {
		atomic.AddInt64(&loaderCalls, 1)
		return testUser{ID: "1", Name: "otmane"}, nil
	}

	first, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1", loader)
	if err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}
	second, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1", loader)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if first != second {
		t.Fatalf("got %+v and %+v, want the same cached value", first, second)
	}
	if calls := atomic.LoadInt64(&loaderCalls); calls != 1 {
		t.Fatalf("loader called %d times, want 1 (second call should be served from redis, with no local tier to check first)", calls)
	}
}

func TestGetOrLoad_Generic_ComposesWithWithoutRedis(t *testing.T) {
	// nil redis client: the strongest proof redis is never touched — see
	// TestTieredCache_WithoutRedis_NeverTouchesRedis for the same reasoning
	// at the byte level.
	tc := NewTieredCache(nil, time.Minute, time.Minute, WithoutRedis())
	t.Cleanup(func() { _ = tc.Close() })
	ctx := context.Background()

	var loaderCalls int64
	loader := func(context.Context) (testUser, error) {
		atomic.AddInt64(&loaderCalls, 1)
		return testUser{ID: "1", Name: "otmane"}, nil
	}

	first, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1", loader)
	if err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}
	second, err := GetOrLoad(ctx, tc, JSONMarshaler[testUser]{}, "user:1", loader)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if first != second {
		t.Fatalf("got %+v and %+v, want the same cached value", first, second)
	}
	if calls := atomic.LoadInt64(&loaderCalls); calls != 1 {
		t.Fatalf("loader called %d times, want 1 (second call should be served from the local tier)", calls)
	}
}
