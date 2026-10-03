package strata

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLocalCache_EnforcesMaxSize(t *testing.T) {
	c := newLocalCache(3, time.Hour)
	defer c.Close()

	for i := range 10 {
		c.Set(fmt.Sprintf("key-%d", i), i, time.Minute)
		if got := c.Len(); got > 3 {
			t.Fatalf("after inserting key-%d: expected at most 3 entries, got %d", i, got)
		}
	}
}

func TestLocalCache_UpdatingExistingKeyDoesNotEvict(t *testing.T) {
	c := newLocalCache(2, time.Hour)
	defer c.Close()

	c.Set("a", 1, time.Minute)
	c.Set("b", 2, time.Minute)
	c.Set("a", 3, time.Minute) // update, at capacity: must not evict "b"

	if _, ok := c.Get("b"); !ok {
		t.Fatal("expected b to still be present after updating a")
	}
	if v, ok := c.Get("a"); !ok || v != 3 {
		t.Fatalf("expected a to be updated to 3, got %v, ok=%v", v, ok)
	}
}

func TestLocalCache_ZeroMaxSizeIsUnbounded(t *testing.T) {
	c := newLocalCache(0, time.Hour)
	defer c.Close()

	for i := range 50 {
		c.Set(fmt.Sprintf("key-%d", i), i, time.Minute)
	}

	if got := c.Len(); got != 50 {
		t.Fatalf("expected all 50 entries to be kept, got %d", got)
	}
}

func TestLocalCache_CloseIsIdempotent(t *testing.T) {
	c := newLocalCache(10, time.Hour)
	c.Close()
	c.Close() // must not panic
}

// Regression test: Get, sweepExpired, and evictOne each remove an entry and
// decrement size in two separate steps. Delete alone can't report whether it
// actually removed anything, so two goroutines racing to expire/evict the
// same key could each decrement size once for what's really a single
// eviction, drifting size negative. All three now use LoadAndDelete and only
// decrement when it reports loaded=true.
func TestLocalCache_ConcurrentExpiryDoesNotCorruptSize(t *testing.T) {
	c := newLocalCache(100, time.Hour) // long sweep interval: isolate the Get-path race
	defer c.Close()

	c.Set("key", 1, time.Millisecond)
	time.Sleep(5 * time.Millisecond) // guarantee it's expired

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Get("key")
		}()
	}
	wg.Wait()

	if got := c.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0 (size counter corrupted by concurrent expiry deletes)", got)
	}
}

// Regression test: Set used to call Swap (making the new entry visible to
// other goroutines) before incrementing size. That left a window where a
// concurrent Delete on the same, just-inserted key could observe the entry
// and decrement size before Set's own increment ran, which is enough for
// Len() to be observed negative, wrong in a way "briefly stale" doesn't
// excuse, since a cache can't hold a negative number of entries. Set now
// increments speculatively before Swap and undoes it if the key turned out
// to already exist (an update, not an insert).
func TestLocalCache_ConcurrentSetDeleteDoesNotCorruptSize(t *testing.T) {
	c := newLocalCache(0, time.Hour) // unbounded: isolate the Set/Delete race from capacity eviction
	defer c.Close()

	const keyspace = 4 // small on purpose: forces repeated collisions on the same keys
	stop := make(chan struct{})
	negative := make(chan int, 1)

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("key-%d", (id+i)%keyspace)
				c.Set(key, i, time.Hour)
				c.Delete(key)
				if got := c.Len(); got < 0 {
					select {
					case negative <- got:
					default:
					}
				}
			}
		}(w)
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	select {
	case got := <-negative:
		t.Fatalf("Len() went negative (%d): Set's increment raced a concurrent Delete on the same fresh key", got)
	default:
	}
}

// observerFunc adapts individual functions to the Observer interface for
// tests that only care about one event. Embedding NoopObserver means an
// unset field falls through to the no-op default, and, same as in
// production code, Observer growing a new method later wouldn't break this
// type either.
type observerFunc struct {
	NoopObserver
	onRedisError  func(error)
	onEncodeError func(error)
	onDecodeError func(error)
	onSetError    func(error)
}

func (o observerFunc) OnRedisError(err error) {
	if o.onRedisError != nil {
		o.onRedisError(err)
	}
}

func (o observerFunc) OnEncodeError(err error) {
	if o.onEncodeError != nil {
		o.onEncodeError(err)
	}
}

func (o observerFunc) OnDecodeError(err error) {
	if o.onDecodeError != nil {
		o.onDecodeError(err)
	}
}

func (o observerFunc) OnSetError(err error) {
	if o.onSetError != nil {
		o.onSetError(err)
	}
}

func newMiniredisTieredCache(t *testing.T, opts ...Option) (*TieredCache, *redis.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("could not start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	tc := NewTieredCache(client, time.Minute, time.Minute, opts...)
	t.Cleanup(func() { _ = tc.Close() })
	return tc, client
}

func TestTieredCache_Get_PlainMissDoesNotTriggerErrorHandler(t *testing.T) {
	called := false
	tc, _ := newMiniredisTieredCache(t, WithObserver(observerFunc{onRedisError: func(error) { called = true }}))

	if _, ok := tc.Get(context.Background(), "does-not-exist"); ok {
		t.Fatal("expected a cache miss")
	}
	if called {
		t.Fatal("error handler should not fire on a plain cache miss")
	}
}

func TestTieredCache_Get_RedisFailureTriggersErrorHandler(t *testing.T) {
	// Nothing listens on this port, so every redis call fails outright; a
	// short deadline keeps the test fast regardless of environment.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close() //nolint:errcheck // This is just a test file ffs

	var gotErr error
	tc := NewTieredCache(client, time.Minute, time.Minute, WithObserver(observerFunc{onRedisError: func(err error) {
		gotErr = err
	}}))
	defer tc.Close() //nolint:errcheck // This is just a test file ffs

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, ok := tc.Get(ctx, "key"); ok {
		t.Fatal("expected a miss when redis is unreachable")
	}
	if gotErr == nil {
		t.Fatal("expected the redis error handler to be invoked")
	}
}

// Regression guard: GetOrLoad must fail open when caching the loaded value
// fails, the loader already did the real work, so a redis outage on the
// write path shouldn't fail the caller's request on top of it. An earlier
// version of this code returned the Set error from GetOrLoad directly,
// which meant a redis write outage failed every GetOrLoad call in the
// service even though the underlying data was fully available.
func TestTieredCache_GetOrLoad_SetFailureFailsOpen(t *testing.T) {
	// Nothing listens on this port, so every redis call fails outright; a
	// short deadline keeps the test fast regardless of environment.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close() //nolint:errcheck // This is just a test file ffs

	var gotErr error
	tc := NewTieredCache(client, time.Minute, time.Minute, WithObserver(observerFunc{onSetError: func(err error) {
		gotErr = err
	}}))
	defer tc.Close() //nolint:errcheck // This is just a test file ffs

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	loaderCalled := false
	loader := func(context.Context) ([]byte, error) {
		loaderCalled = true
		return []byte("loaded-value"), nil
	}

	val, err := tc.GetOrLoad(ctx, "key", loader)
	if err != nil {
		t.Fatalf("GetOrLoad returned an error despite a successful loader call: %v", err)
	}
	if !loaderCalled {
		t.Fatal("expected the loader to be called on a miss")
	}
	if string(val) != "loaded-value" {
		t.Fatalf("GetOrLoad returned %q, want the loaded value", val)
	}
	if gotErr == nil {
		t.Fatal("expected the set error handler to be invoked")
	}
}

func TestTieredCache_Invalidate_EvictsBothTiers(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	if err := tc.Set(ctx, key, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := tc.Invalidate(ctx, key); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if _, ok := tc.Get(ctx, key); ok {
		t.Fatal("expected key to be gone after Invalidate")
	}
}

// TestTieredCache_Set_PublishesInvalidation proves Set broadcasts the same
// invalidation Invalidate does: a second instance, backed by the same
// redis and subscribed via SubscribeInvalidations, must drop its stale
// local copy when the first instance overwrites the key with Set, not just
// with Invalidate.
func TestTieredCache_Set_PublishesInvalidation(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("could not start miniredis: %v", err)
	}
	defer mr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const key = "key"

	writer := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer writer.Close() //nolint:errcheck // test cleanup
	writerCache := NewTieredCache(writer, time.Minute, time.Minute)
	defer writerCache.Close() //nolint:errcheck // test cleanup

	reader := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer reader.Close() //nolint:errcheck // test cleanup
	readerCache := NewTieredCache(reader, time.Minute, time.Minute)
	defer readerCache.Close() //nolint:errcheck // test cleanup
	readerCache.SubscribeInvalidations(ctx)

	// Seed the reader's local tier with the stale value, same as if it had
	// read it before the writer's update below.
	if err := readerCache.Set(ctx, key, []byte("stale")); err != nil {
		t.Fatalf("seeding reader: %v", err)
	}

	if err := writerCache.Set(ctx, key, []byte("fresh")); err != nil {
		t.Fatalf("writer Set: %v", err)
	}

	// The invalidation arrives over pubsub asynchronously, so poll instead
	// of asserting immediately.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := readerCache.local.Get(key); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reader's local copy was never evicted after the writer's Set")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTieredCache_Set_DoesNotSelfEvict proves a node that both writes and
// subscribes to its own invalidations, the normal topology when every
// instance both reads and writes, doesn't undo its own Set: redis pub/sub
// delivers a publish back to the publisher's own subscription, so without
// the instanceID tag in invalidationPayload, SubscribeInvalidations would
// immediately evict the value Set just wrote locally.
func TestTieredCache_Set_DoesNotSelfEvict(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"
	tc.SubscribeInvalidations(ctx)

	if err := tc.Set(ctx, key, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Give a buggy self-echo eviction a generous window to happen before
	// declaring the local copy survived.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := tc.local.Get(key); !ok {
			t.Fatal("own Set was evicted by this instance's own invalidation publish")
		}
	}
}

// TestTieredCache_Set_CopiesValue proves Set doesn't retain the caller's
// own slice: mutating value after Set returns, the way reusing a
// sync.Pool buffer would, must not reach the cached copy.
func TestTieredCache_Set_CopiesValue(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	buf := []byte("original")
	if err := tc.Set(ctx, key, buf); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Simulate the caller reusing/mutating its buffer after Set returns.
	for i := range buf {
		buf[i] = 'X'
	}

	got, ok := tc.Get(ctx, key)
	if !ok {
		t.Fatal("expected a hit")
	}
	if string(got) != "original" {
		t.Fatalf("Get = %q after mutating the caller's buffer, want %q, Set must have copied it", got, "original")
	}
}

// TestTieredCache_Get_RedisHitPromotion_CopiesIntoLocal proves a redis-hit
// promotion doesn't hand the caller the exact slice it just stored in the
// local tier: mutating what Get returns must not corrupt the promoted
// local copy a later Get serves.
func TestTieredCache_Get_RedisHitPromotion_CopiesIntoLocal(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	if err := tc.Set(ctx, key, []byte("original")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Force the next Get through the redis-hit promotion path.
	tc.local.Delete(key)

	got, ok := tc.Get(ctx, key)
	if !ok {
		t.Fatal("expected a hit")
	}
	for i := range got {
		got[i] = 'X'
	}

	again, ok := tc.Get(ctx, key)
	if !ok {
		t.Fatal("expected a hit")
	}
	if string(again) != "original" {
		t.Fatalf("Get = %q after mutating a prior redis-hit result, want %q, the local promotion must have copied it", again, "original")
	}
}

// TestTieredCache_GetOrLoad_CopiesLoaderResult proves GetOrLoad doesn't
// hand the caller the exact slice it just cached from the loader's
// result: mutating the returned value must not corrupt what's now cached.
func TestTieredCache_GetOrLoad_CopiesLoaderResult(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	loader := func(context.Context) ([]byte, error) { return []byte("original"), nil }
	val, err := tc.GetOrLoad(ctx, key, loader)
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	for i := range val {
		val[i] = 'X'
	}

	got, ok := tc.Get(ctx, key)
	if !ok {
		t.Fatal("expected a hit")
	}
	if string(got) != "original" {
		t.Fatalf("Get = %q after mutating GetOrLoad's returned value, want %q, the cache write must have copied it", got, "original")
	}
}

// TestTieredCache_GetOrLoad_SlowLoaderDoesNotOverwriteNewerSet reproduces
// the race a slow loader can hit: GetOrLoad's loader is still running when
// a concurrent Set for the same key completes with a fresher value. Once
// the slow loader finally returns its now-stale result, GetOrLoad must not
// let it clobber what the concurrent Set already wrote, there would be
// nothing left to correct it afterward. See setIfNewer's doc comment.
func TestTieredCache_GetOrLoad_SlowLoaderDoesNotOverwriteNewerSet(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	loaderStarted := make(chan struct{})
	resumeLoader := make(chan struct{})
	loader := func(context.Context) ([]byte, error) { //nolint:unparam // error is part of the loader signature GetOrLoad requires, this double never fails
		close(loaderStarted)
		<-resumeLoader
		return []byte("stale"), nil
	}

	loadDone := make(chan struct{})
	var loadedVal []byte
	var loadErr error
	go func() {
		defer close(loadDone)
		loadedVal, loadErr = tc.GetOrLoad(ctx, key, loader)
	}()

	<-loaderStarted

	// A concurrent write lands while the loader above is still running.
	if err := tc.Set(ctx, key, []byte("fresh")); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	close(resumeLoader)
	<-loadDone

	if loadErr != nil {
		t.Fatalf("GetOrLoad: %v", loadErr)
	}
	if string(loadedVal) != "stale" {
		t.Fatalf("GetOrLoad returned %q, want the loader's own result %q", loadedVal, "stale")
	}

	// The cache itself must still hold the fresher value: the loader's
	// stale result must never have overwritten it. Force a redis read,
	// bypassing whatever Set already put in the local tier, to check
	// redis specifically, the tier this race would otherwise corrupt for
	// good.
	tc.local.Delete(key)
	got, ok := tc.Get(ctx, key)
	if !ok {
		t.Fatal("expected a hit")
	}
	if string(got) != "fresh" {
		t.Fatalf("Get = %q after the race, want %q, the stale loader result must not have overwritten it", got, "fresh")
	}

	if got := tc.Stats().StaleWritesDropped; got != 1 {
		t.Errorf("StaleWritesDropped = %d, want 1", got)
	}
}

func TestNewTieredCache_PanicsWhenBothTiersDisabled(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected NewTieredCache to panic when both tiers are disabled")
		}
	}()
	NewTieredCache(nil, time.Minute, time.Minute, WithoutLocalCache(), WithoutRedis())
}

func TestTieredCache_WithoutLocalCache_BypassesLocalTier(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t, WithoutLocalCache())
	ctx := context.Background()
	const key = "key"

	if tc.local != nil {
		t.Fatal("expected the local tier to not be allocated when WithoutLocalCache is used")
	}

	if err := tc.Set(ctx, key, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if val, ok := tc.Get(ctx, key); !ok || string(val) != "value" {
		t.Fatalf("Get = %q, ok=%v, want %q, true", val, ok, "value")
	}
}

// TestTieredCache_WithoutRedis_NeverTouchesRedis passes a nil redis client,
// the strongest possible proof that redis is never dialed in this mode: any
// code path that touched tc.redis would nil-pointer-panic immediately.
func TestTieredCache_WithoutRedis_NeverTouchesRedis(t *testing.T) {
	tc := NewTieredCache(nil, time.Minute, time.Minute, WithoutRedis())
	defer tc.Close() //nolint:errcheck // test cleanup
	ctx := context.Background()
	const key = "key"

	if err := tc.Set(ctx, key, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if val, ok := tc.Get(ctx, key); !ok || string(val) != "value" {
		t.Fatalf("Get = %q, ok=%v, want %q, true", val, ok, "value")
	}
	if err := tc.Invalidate(ctx, key); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if _, ok := tc.Get(ctx, key); ok {
		t.Fatal("expected key to be gone after Invalidate")
	}

	// SubscribeInvalidations must also be a safe no-op: there's no redis to
	// subscribe to.
	tc.SubscribeInvalidations(ctx)
}

// prefixCodec is a minimal reversible Codec for tests: Encode prepends a
// marker byte, Decode requires and strips it. This makes it possible to
// assert, from outside the package's internals, whether a given []byte is
// the encoded (wire) form or the decoded (original) form.
type prefixCodec struct{ marker byte }

func (c prefixCodec) Encode(value []byte) ([]byte, error) {
	return append([]byte{c.marker}, value...), nil
}

func (c prefixCodec) Decode(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != c.marker {
		return nil, fmt.Errorf("prefixCodec: missing marker byte %#x", c.marker)
	}
	return data[1:], nil
}

func TestTieredCache_Codec_RoundTrip(t *testing.T) {
	tc, client := newMiniredisTieredCache(t, WithCodec(prefixCodec{marker: 0xAB}))
	ctx := context.Background()
	const key = "key"

	if err := tc.Set(ctx, key, []byte("hello")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// redis should hold the ENCODED form, behind a version prefix.
	raw, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("reading raw redis value: %v", err)
	}
	encoded := stripVersion(raw)
	if len(encoded) == 0 || encoded[0] != 0xAB {
		t.Fatalf("expected encoded bytes in redis, got %v", encoded)
	}

	// Force the local tier to miss, so this Get goes through the
	// redis+decode path and promotes into local.
	tc.local.Delete(key)

	first, ok := tc.Get(ctx, key)
	if !ok || string(first) != "hello" {
		t.Fatalf("cold Get: got %q, ok=%v", first, ok)
	}

	// Regression check: a prior version of this code promoted the
	// still-encoded bytes into the local tier, so this second, warm call
	// would return "\xABhello" instead of "hello".
	second, ok := tc.Get(ctx, key)
	if !ok || string(second) != "hello" {
		t.Fatalf("warm Get: got %q, ok=%v", second, ok)
	}
}

func TestTieredCache_Codec_EncodeFailurePreventsWrite(t *testing.T) {
	boom := errors.New("boom")
	failingCodec := codecFunc{
		encode: func([]byte) ([]byte, error) { return nil, boom },
		decode: func(data []byte) ([]byte, error) { return data, nil },
	}

	var gotErr error
	tc, client := newMiniredisTieredCache(
		t,
		WithCodec(failingCodec),
		WithObserver(observerFunc{onEncodeError: func(err error) { gotErr = err }}),
	)
	ctx := context.Background()

	if err := tc.Set(ctx, "key", []byte("hello")); err == nil {
		t.Fatal("expected Set to return the encode error")
	}
	if !errors.Is(gotErr, boom) {
		t.Fatalf("expected the encode error handler to observe %v, got %v", boom, gotErr)
	}
	if exists, err := client.Exists(ctx, "key").Result(); err != nil || exists != 0 {
		t.Fatalf("expected nothing written to redis after a failed encode, exists=%d err=%v", exists, err)
	}
}

func TestTieredCache_Codec_DecodeFailureIsAMiss(t *testing.T) {
	// Start plain (no codec) so we can seed redis directly with data that
	// won't decode, simulating e.g. entries written before a codec was
	// introduced.
	_, client := newMiniredisTieredCache(t)
	ctx := context.Background()
	const key = "key"

	if err := client.Set(ctx, key, []byte("not-encoded"), time.Minute).Err(); err != nil {
		t.Fatalf("seeding redis: %v", err)
	}

	var gotErr error
	tc := NewTieredCache(
		client, time.Minute, time.Minute,
		WithCodec(prefixCodec{marker: 0xAB}),
		WithObserver(observerFunc{onDecodeError: func(err error) { gotErr = err }}),
	)
	defer tc.Close() //nolint:errcheck // test cleanup

	if _, ok := tc.Get(ctx, key); ok {
		t.Fatal("expected a miss when the codec can't decode the stored value")
	}
	if gotErr == nil {
		t.Fatal("expected the decode error handler to be invoked")
	}
}

// codecFunc adapts two functions to the Codec interface for tests that need
// specific failure behavior prefixCodec can't express.
type codecFunc struct {
	encode func([]byte) ([]byte, error)
	decode func([]byte) ([]byte, error)
}

func (c codecFunc) Encode(value []byte) ([]byte, error) { return c.encode(value) }
func (c codecFunc) Decode(data []byte) ([]byte, error)  { return c.decode(data) }

// TestTieredCache_Stats exercises every Stats counter through the code path
// that's supposed to increment it, one at a time, and checks nothing else
// there.
func TestTieredCache_Stats(t *testing.T) {
	t.Run("local and redis hits and misses", func(t *testing.T) {
		tc, _ := newMiniredisTieredCache(t)
		ctx := context.Background()
		const key = "key"

		// Redis miss: nothing has been written for this key anywhere yet.
		if _, ok := tc.Get(ctx, key); ok {
			t.Fatal("expected a miss")
		}

		if err := tc.Set(ctx, key, []byte("value")); err != nil {
			t.Fatalf("Set: %v", err)
		}

		// Local hit: Set already populated the local tier.
		if _, ok := tc.Get(ctx, key); !ok {
			t.Fatal("expected a hit")
		}

		// Redis hit: force the local tier to miss so this Get falls
		// through to redis.
		tc.local.Delete(key)
		if _, ok := tc.Get(ctx, key); !ok {
			t.Fatal("expected a hit")
		}

		stats := tc.Stats()
		if stats.LocalHits != 1 {
			t.Errorf("LocalHits = %d, want 1", stats.LocalHits)
		}
		if stats.LocalMisses != 2 { // the very first Get, and the forced-cold one
			t.Errorf("LocalMisses = %d, want 2", stats.LocalMisses)
		}
		if stats.RedisHits != 1 {
			t.Errorf("RedisHits = %d, want 1", stats.RedisHits)
		}
		if stats.RedisMisses != 1 {
			t.Errorf("RedisMisses = %d, want 1", stats.RedisMisses)
		}
	})

	t.Run("redis error", func(t *testing.T) {
		// Nothing listens on this port, so every redis call fails outright;
		// a short deadline keeps the test fast regardless of environment.
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer client.Close() //nolint:errcheck // test cleanup
		tc := NewTieredCache(client, time.Minute, time.Minute)
		defer tc.Close() //nolint:errcheck // test cleanup

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		if _, ok := tc.Get(ctx, "key"); ok {
			t.Fatal("expected a miss")
		}

		if got := tc.Stats().RedisErrors; got != 1 {
			t.Errorf("RedisErrors = %d, want 1", got)
		}
	})

	t.Run("encode error", func(t *testing.T) {
		boom := errors.New("boom")
		failingCodec := codecFunc{
			encode: func([]byte) ([]byte, error) { return nil, boom },
			decode: func(data []byte) ([]byte, error) { return data, nil },
		}
		tc, _ := newMiniredisTieredCache(t, WithCodec(failingCodec))
		ctx := context.Background()

		if err := tc.Set(ctx, "key", []byte("hello")); err == nil {
			t.Fatal("expected Set to return the encode error")
		}

		if got := tc.Stats().EncodeErrors; got != 1 {
			t.Errorf("EncodeErrors = %d, want 1", got)
		}
	})

	t.Run("decode error", func(t *testing.T) {
		// Start plain (no codec) so redis can be seeded directly with data
		// that won't decode, simulating e.g. entries written before a
		// codec was introduced.
		_, client := newMiniredisTieredCache(t)
		ctx := context.Background()
		const key = "key"

		if err := client.Set(ctx, key, []byte("not-encoded"), time.Minute).Err(); err != nil {
			t.Fatalf("seeding redis: %v", err)
		}

		tc := NewTieredCache(client, time.Minute, time.Minute, WithCodec(prefixCodec{marker: 0xAB}))
		defer tc.Close() //nolint:errcheck // test cleanup

		if _, ok := tc.Get(ctx, key); ok {
			t.Fatal("expected a miss")
		}

		if got := tc.Stats().DecodeErrors; got != 1 {
			t.Errorf("DecodeErrors = %d, want 1", got)
		}
	})

	t.Run("set error inside GetOrLoad", func(t *testing.T) {
		// Nothing listens on this port, so every redis call fails outright;
		// a short deadline keeps the test fast regardless of environment.
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer client.Close() //nolint:errcheck // test cleanup
		tc := NewTieredCache(client, time.Minute, time.Minute)
		defer tc.Close() //nolint:errcheck // test cleanup

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		loader := func(context.Context) ([]byte, error) { return []byte("value"), nil }
		if _, err := tc.GetOrLoad(ctx, "key", loader); err != nil {
			t.Fatalf("GetOrLoad returned an error despite a successful loader call: %v", err)
		}

		if got := tc.Stats().SetErrors; got != 1 {
			t.Errorf("SetErrors = %d, want 1", got)
		}
	})
}

// fakeHistogram is a Histogram that just counts how many observations it
// received, which is all these tests need, not the values themselves.
// Observe is called inline on the request path from concurrent callers in
// some tests, so it guards the counter with a mutex.
type fakeHistogram struct {
	mu    sync.Mutex
	count int
}

func (h *fakeHistogram) Observe(float64) {
	h.mu.Lock()
	h.count++
	h.mu.Unlock()
}

func (h *fakeHistogram) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func TestTieredCache_LatencyHistograms(t *testing.T) {
	t.Run("records local, redis, and pubsub latency on Get and Set", func(t *testing.T) {
		local := &fakeHistogram{}
		redisHist := &fakeHistogram{}
		pubsub := &fakeHistogram{}
		tc, _ := newMiniredisTieredCache(t,
			WithLocalLatencyHistogram(local),
			WithRedisLatencyHistogram(redisHist),
			WithPubSubLatencyHistogram(pubsub),
		)
		ctx := context.Background()
		const key = "key"

		// Set: one local observation, one redis observation (the write
		// itself), one pubsub observation (publishing the invalidation).
		if err := tc.Set(ctx, key, []byte("value")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if got := local.Count(); got != 1 {
			t.Errorf("after Set, local histogram count = %d, want 1", got)
		}
		if got := redisHist.Count(); got != 1 {
			t.Errorf("after Set, redis histogram count = %d, want 1", got)
		}
		if got := pubsub.Count(); got != 1 {
			t.Errorf("after Set, pubsub histogram count = %d, want 1", got)
		}

		// Local hit: one more local observation, no redis or pubsub call
		// at all.
		if _, ok := tc.Get(ctx, key); !ok {
			t.Fatal("expected a hit")
		}
		if got := local.Count(); got != 2 {
			t.Errorf("after local-hit Get, local histogram count = %d, want 2", got)
		}
		if got := redisHist.Count(); got != 1 {
			t.Errorf("after local-hit Get, redis histogram count = %d, want 1", got)
		}
		if got := pubsub.Count(); got != 1 {
			t.Errorf("after local-hit Get, pubsub histogram count = %d, want 1", got)
		}

		// Force a redis round trip: one more local observation (the miss
		// check), one more redis observation, and one more local
		// observation (the redis-hit promotion back into the local tier).
		// Get never publishes, so pubsub stays unchanged.
		tc.local.Delete(key)
		if _, ok := tc.Get(ctx, key); !ok {
			t.Fatal("expected a hit")
		}
		if got := local.Count(); got != 4 {
			t.Errorf("after redis-hit Get, local histogram count = %d, want 4", got)
		}
		if got := redisHist.Count(); got != 2 {
			t.Errorf("after redis-hit Get, redis histogram count = %d, want 2", got)
		}
		if got := pubsub.Count(); got != 1 {
			t.Errorf("after redis-hit Get, pubsub histogram count = %d, want 1", got)
		}
	})

	t.Run("nil histograms are left unset by default", func(t *testing.T) {
		// No WithLocalLatencyHistogram / WithRedisLatencyHistogram: Get and
		// Set must not panic on a nil Histogram.
		tc, _ := newMiniredisTieredCache(t)
		ctx := context.Background()

		if err := tc.Set(ctx, "key", []byte("value")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if _, ok := tc.Get(ctx, "key"); !ok {
			t.Fatal("expected a hit")
		}
	})
}

// TestWithCache_MemoizesFunction verifies WithCache's whole reason to
// exist: a second call with the same args must be served from the cache
// instead of calling the wrapped function again. It also checks that
// different args still reach the function, so the test can't be satisfied
// by an implementation that just never calls fn a second time regardless
// of args.
func TestWithCache_MemoizesFunction(t *testing.T) {
	tc, _ := newMiniredisTieredCache(t)
	ctx := context.Background()

	calls := map[string]int{}
	getUser := WithCache(tc, JSONMarshaler[exampleUser]{},
		func(id string) string { return "user:" + id },
		func(_ context.Context, id string) (exampleUser, error) {
			calls[id]++
			return exampleUser{ID: id, Name: "otmane"}, nil
		},
	)

	first, err := getUser(ctx, "1234")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := getUser(ctx, "1234")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if first != second {
		t.Fatalf("first = %+v, second = %+v, want equal", first, second)
	}
	if got := calls["1234"]; got != 1 {
		t.Fatalf("fn called %d times for id 1234, want 1 (second call should be served from cache)", got)
	}

	if _, err := getUser(ctx, "5678"); err != nil {
		t.Fatalf("call with a different id: %v", err)
	}
	if got := calls["5678"]; got != 1 {
		t.Fatalf("fn called %d times for id 5678, want 1 (a different key must still reach fn)", got)
	}
}
