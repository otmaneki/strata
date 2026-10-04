package strata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// localExpiry reads key's local-tier expiry directly, so TTL assertions
// don't have to sleep out a real duration to observe one.
func localExpiry(t *testing.T, tc *TieredCache, key string) time.Time {
	t.Helper()

	raw, ok := tc.local.data.Load(key)
	if !ok {
		t.Fatalf("key %s is not in the local tier", key)
	}
	e, ok := raw.(*entry)
	if !ok {
		t.Fatalf("local tier holds %T under key %s, want *entry", raw, key)
	}
	return e.expiresAt
}

// assertLocalTTL checks key expires locally about ttl from now. The
// bound is the window the write itself could have happened in, not a
// fudge factor: expiresAt was computed from a time.Now() inside Set.
func assertLocalTTL(t *testing.T, tc *TieredCache, key string, before time.Time, ttl time.Duration) {
	t.Helper()

	got := localExpiry(t, tc, key)
	earliest, latest := before.Add(ttl), time.Now().Add(ttl)
	if got.Before(earliest) || got.After(latest) {
		t.Fatalf("local expiry for %s = %v, want within [%v, %v] (ttl %v)", key, got, earliest, latest, ttl)
	}
}

// assertLocalTTLAtMost checks key expires locally within (0, ttl] from
// now, rather than at a specific point.
//
// Promotion caps the local TTL at the key's *remaining* redis lifetime,
// which has already decayed by the time the read happens, so the exact
// bound assertLocalTTL uses doesn't hold: asking for a 10s cap and
// getting 9.998s is correct behavior, not drift.
func assertLocalTTLAtMost(t *testing.T, tc *TieredCache, key string, ttl time.Duration) {
	t.Helper()

	remaining := time.Until(localExpiry(t, tc, key))
	if remaining <= 0 || remaining > ttl {
		t.Fatalf("local entry for %s expires in %v, want within (0, %v]", key, remaining, ttl)
	}
}

// assertRemoteTTL checks key's redis PTTL is in (0, ttl]. It can only
// have shrunk since the write, never grown, so an upper bound of exactly
// ttl is tight rather than flaky.
func assertRemoteTTL(t *testing.T, client redis.UniversalClient, key string, ttl time.Duration) {
	t.Helper()

	got, err := client.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("PTTL %s: %v", key, err)
	}
	if got <= 0 || got > ttl {
		t.Fatalf("redis PTTL for %s = %v, want within (0, %v]", key, got, ttl)
	}
}

func TestResolveTTLs(t *testing.T) {
	const (
		defaultLocal  = time.Minute
		defaultRemote = 5 * time.Minute
	)
	tc := &TieredCache{localTTL: defaultLocal, remoteTTL: defaultRemote}

	tests := []struct {
		name               string
		opts               []WriteOption
		wantLocal, wantRem time.Duration
	}{
		{
			name:      "no options keeps both defaults",
			opts:      nil,
			wantLocal: defaultLocal,
			wantRem:   defaultRemote,
		},
		{
			name:      "TTL sets both tiers",
			opts:      []WriteOption{TTL(time.Hour)},
			wantLocal: time.Hour,
			wantRem:   time.Hour,
		},
		{
			name:      "LocalTTL leaves remote on its default",
			opts:      []WriteOption{LocalTTL(time.Second)},
			wantLocal: time.Second,
			wantRem:   defaultRemote,
		},
		{
			name:      "RemoteTTL leaves local on its default",
			opts:      []WriteOption{RemoteTTL(7 * 24 * time.Hour)},
			wantLocal: defaultLocal,
			wantRem:   7 * 24 * time.Hour,
		},
		{
			name:      "both tiers set explicitly",
			opts:      []WriteOption{LocalTTL(time.Second), RemoteTTL(time.Hour)},
			wantLocal: time.Second,
			wantRem:   time.Hour,
		},
		{
			name:      "LocalTTL overrides TTL for its own tier only",
			opts:      []WriteOption{TTL(30 * time.Minute), LocalTTL(time.Minute)},
			wantLocal: time.Minute,
			wantRem:   30 * time.Minute,
		},
		{
			// The point of resolving into a struct: a specific option
			// beats TTL even when TTL is applied last.
			name:      "precedence does not depend on option order",
			opts:      []WriteOption{LocalTTL(time.Minute), TTL(30 * time.Minute)},
			wantLocal: time.Minute,
			wantRem:   30 * time.Minute,
		},
		{
			// The default local TTL (1m) outlives the resolved remote
			// one (1s) here, so the cap pulls it down to 1s.
			name:      "last option of the same kind wins",
			opts:      []WriteOption{RemoteTTL(time.Hour), RemoteTTL(time.Second)},
			wantLocal: time.Second,
			wantRem:   time.Second,
		},
		{
			name:      "local is capped to remote",
			opts:      []WriteOption{LocalTTL(time.Hour), RemoteTTL(time.Minute)},
			wantLocal: time.Minute,
			wantRem:   time.Minute,
		},
		{
			// Equal is not greater: nothing to cap.
			name:      "local equal to remote is untouched",
			opts:      []WriteOption{TTL(time.Hour)},
			wantLocal: time.Hour,
			wantRem:   time.Hour,
		},
		{
			// A non-positive remote TTL means the key never expires in
			// redis, so there's no remote lifetime to cap against.
			name:      "no cap when remote never expires",
			opts:      []WriteOption{LocalTTL(time.Hour), RemoteTTL(0)},
			wantLocal: time.Hour,
			wantRem:   0,
		},
		{
			// Zero is a real choice, not "unset": it must survive
			// resolution rather than fall back to the default.
			name:      "zero is distinguishable from unspecified",
			opts:      []WriteOption{LocalTTL(0), RemoteTTL(0)},
			wantLocal: 0,
			wantRem:   0,
		},
		{
			name:      "nil options are skipped",
			opts:      []WriteOption{nil, TTL(time.Hour), nil},
			wantLocal: time.Hour,
			wantRem:   time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local, remote := tc.resolveTTLs(tt.opts)
			if local != tt.wantLocal {
				t.Errorf("local TTL = %v, want %v", local, tt.wantLocal)
			}
			if remote != tt.wantRem {
				t.Errorf("remote TTL = %v, want %v", remote, tt.wantRem)
			}
		})
	}

	// Resolution must not mutate the cache's configured defaults; a
	// per-write override that leaked would silently retune every
	// subsequent write.
	if tc.localTTL != defaultLocal || tc.remoteTTL != defaultRemote {
		t.Fatalf("defaults mutated: localTTL=%v remoteTTL=%v", tc.localTTL, tc.remoteTTL)
	}
}

// TestResolveTTLs_CapsConfiguredDefaults covers the cap applying to
// NewTieredCache's own TTLs, not just to WriteOptions: the constructor
// accepts a localTTL longer than its remoteTTL, and that pairing strands
// a local copy exactly the same way.
func TestResolveTTLs_CapsConfiguredDefaults(t *testing.T) {
	tc := &TieredCache{localTTL: 5 * time.Minute, remoteTTL: 30 * time.Second}

	local, remote := tc.resolveTTLs(nil)
	if local != 30*time.Second {
		t.Errorf("local TTL = %v, want it capped to the 30s remote TTL", local)
	}
	if remote != 30*time.Second {
		t.Errorf("remote TTL = %v, want 30s", remote)
	}
}

func TestTTLMilliseconds(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{"whole milliseconds are exact", 1500 * time.Millisecond, 1500},
		{"exactly one millisecond", time.Millisecond, 1},
		// The regression this rounding exists for: truncation sent any
		// sub-millisecond TTL to 0, which the Lua scripts read as "no
		// expiry". Asking for 500µs and getting an immortal key is the
		// worst direction to be wrong in.
		{"sub-millisecond rounds up, never to zero", 500 * time.Microsecond, 1},
		{"one nanosecond rounds up", time.Nanosecond, 1},
		{"fractional milliseconds round up", 1500 * time.Microsecond, 2},
		// Zero and below keep meaning "no expiry", deliberately.
		{"zero stays zero", 0, 0},
		{"negative stays zero", -time.Second, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ttlMilliseconds(tt.in); got != tt.want {
				t.Errorf("ttlMilliseconds(%v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestTieredCache_PromotionTTL(t *testing.T) {
	tc := &TieredCache{localTTL: time.Minute}

	tests := []struct {
		name string
		pttl time.Duration
		err  error
		want time.Duration
	}{
		{"shorter remote lifetime caps the promotion", 10 * time.Second, nil, 10 * time.Second},
		{"longer remote lifetime leaves localTTL alone", time.Hour, nil, time.Minute},
		// go-redis reports PTTL's -1/-2 sentinels as raw durations, not
		// scaled by its millisecond precision.
		{"no expiry leaves localTTL alone", -1, nil, time.Minute},
		{"already gone means do not promote", -2, nil, 0},
		{"a PTTL error falls back to localTTL", 0, errors.New("boom"), time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tc.promotionTTL(tt.pttl, tt.err); got != tt.want {
				t.Errorf("promotionTTL(%v, %v) = %v, want %v", tt.pttl, tt.err, got, tt.want)
			}
		})
	}
}

func TestTieredCache_Set_CapsLocalTTLToRemote(t *testing.T) {
	tc, client := newTestTieredCache(t) // both tiers default to a minute
	ctx := context.Background()

	// Asking for an hour locally against a 10s remote lifetime: the
	// local copy would otherwise outlive redis by 59 minutes, with
	// nothing left to correct or evict it.
	before := time.Now()
	if err := tc.Set(ctx, "otp:123", []byte("424242"), LocalTTL(time.Hour), RemoteTTL(10*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "otp:123", before, 10*time.Second)
	assertRemoteTTL(t, client, "otp:123", 10*time.Second)
}

// TestTieredCache_Get_PromotionRespectsRemainingRemoteTTL is the
// cross-node half of the same guarantee. A per-write RemoteTTL exists
// only in the writer's call, so a reader has to recover it from the key's
// own remaining lifetime or it will cache the value for its full default
// localTTL.
func TestTieredCache_Get_PromotionRespectsRemainingRemoteTTL(t *testing.T) {
	tc, _ := newTestTieredCache(t) // localTTL default: one minute
	ctx := context.Background()

	if err := tc.Set(ctx, "otp:456", []byte("131313"), RemoteTTL(10*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Drop the local copy so the next Get has to come back through
	// redis, which is the path another node would take.
	tc.local.Delete("otp:456")

	val, ok := tc.Get(ctx, "otp:456")
	if !ok {
		t.Fatal("expected a redis hit")
	}
	if string(val) != "131313" {
		t.Fatalf("Get = %q, want %q", val, "131313")
	}

	// Promoted for whatever redis has left, at most the 10s it was
	// written with, rather than the 1m default localTTL.
	assertLocalTTLAtMost(t, tc, "otp:456", 10*time.Second)
}

// TestTieredCache_Get_PromotionKeepsLocalTTLForNonExpiringKeys guards the
// other side of promotionTTL: a key with no expiry must not be promoted
// for a negative or zero duration just because PTTL returned -1.
func TestTieredCache_Get_PromotionKeepsLocalTTLForNonExpiringKeys(t *testing.T) {
	tc, _ := newTestTieredCache(t)
	ctx := context.Background()

	if err := tc.Set(ctx, "forever", []byte("value"), RemoteTTL(0)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	tc.local.Delete("forever")

	before := time.Now()
	if _, ok := tc.Get(ctx, "forever"); !ok {
		t.Fatal("expected a redis hit")
	}

	assertLocalTTL(t, tc, "forever", before, time.Minute)
}

func TestTieredCache_Set_NoOptionsUsesConfiguredTTLs(t *testing.T) {
	tc, client := newTestTieredCache(t) // both tiers default to a minute
	ctx := context.Background()

	before := time.Now()
	if err := tc.Set(ctx, "key", []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "key", before, time.Minute)
	assertRemoteTTL(t, client, "key", time.Minute)
}

func TestTieredCache_Set_RemoteTTLOverridesRedisOnly(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	before := time.Now()
	if err := tc.Set(ctx, "config:countries", []byte("value"), RemoteTTL(7*24*time.Hour)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "config:countries", before, time.Minute) // untouched default
	assertRemoteTTL(t, client, "config:countries", 7*24*time.Hour)
}

func TestTieredCache_Set_LocalTTLOverridesLocalOnly(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	before := time.Now()
	if err := tc.Set(ctx, "key", []byte("value"), LocalTTL(10*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "key", before, 10*time.Second)
	assertRemoteTTL(t, client, "key", time.Minute) // untouched default
}

func TestTieredCache_Set_TTLWithLocalOverride(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	before := time.Now()
	err := tc.Set(ctx, "session:abc", []byte("value"), TTL(30*time.Minute), LocalTTL(time.Minute))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "session:abc", before, time.Minute)
	assertRemoteTTL(t, client, "session:abc", 30*time.Minute)
}

func TestTieredCache_Set_NonPositiveRemoteTTLMeansNoExpiry(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	if err := tc.Set(ctx, "forever", []byte("value"), RemoteTTL(0)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// redis reports -1 for a key that exists with no expiry, -2 for one
	// that doesn't exist at all. go-redis surfaces both as durations.
	ttl, err := client.PTTL(ctx, "forever").Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl != -1 {
		t.Fatalf("PTTL = %v, want -1 (no expiry)", ttl)
	}
}

func TestTieredCache_Set_NonPositiveLocalTTLEvictsStaleLocalEntry(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	// Warm the local tier, then rewrite the key as remote-only. Skipping
	// the local write without deleting would leave "stale" readable
	// locally while redis serves "fresh", which is the whole bug this
	// guards.
	if err := tc.Set(ctx, "key", []byte("stale")); err != nil {
		t.Fatalf("seeding Set: %v", err)
	}
	if err := tc.Set(ctx, "key", []byte("fresh"), LocalTTL(0)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, ok := tc.local.Get("key"); ok {
		t.Fatal("local tier still holds the key after a non-positive LocalTTL")
	}

	got, err := client.Get(ctx, "key").Bytes()
	if err != nil {
		t.Fatalf("redis Get: %v", err)
	}
	if string(got) != "fresh" {
		t.Fatalf("redis holds %q, want %q", got, "fresh")
	}

	// And the read path must now serve the fresh value, not the stale
	// local one, promoting it back into the local tier on the way.
	val, ok := tc.Get(ctx, "key")
	if !ok {
		t.Fatal("Get missed a key that is present in redis")
	}
	if string(val) != "fresh" {
		t.Fatalf("Get returned %q, want %q", val, "fresh")
	}
}

func TestTieredCache_Set_WriteOptionsDoNotLeakAcrossCalls(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	if err := tc.Set(ctx, "overridden", []byte("value"), TTL(time.Hour)); err != nil {
		t.Fatalf("Set with options: %v", err)
	}

	before := time.Now()
	if err := tc.Set(ctx, "default", []byte("value")); err != nil {
		t.Fatalf("Set without options: %v", err)
	}

	assertLocalTTL(t, tc, "default", before, time.Minute)
	assertRemoteTTL(t, client, "default", time.Minute)
}

func TestTieredCache_Set_WithoutRedis_HonorsLocalTTL(t *testing.T) {
	tc := NewTieredCache(nil, time.Minute, time.Minute, WithoutRedis())
	t.Cleanup(func() { _ = tc.Close() })

	before := time.Now()
	if err := tc.Set(context.Background(), "key", []byte("value"), TTL(10*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertLocalTTL(t, tc, "key", before, 10*time.Second)
}

func TestTieredCache_GetOrLoad_AppliesWriteOptions(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	before := time.Now()
	loader := func(context.Context) ([]byte, error) { return []byte("loaded"), nil }

	val, err := tc.GetOrLoad(ctx, "config:countries", loader, RemoteTTL(7*24*time.Hour))
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if string(val) != "loaded" {
		t.Fatalf("GetOrLoad returned %q, want %q", val, "loaded")
	}

	// GetOrLoad's miss goes through setIfVersion, not Set, so this is a
	// genuinely separate write path from the Set tests above.
	assertLocalTTL(t, tc, "config:countries", before, time.Minute)
	assertRemoteTTL(t, client, "config:countries", 7*24*time.Hour)
}

func TestTieredCache_GetOrLoad_WithoutRedis_AppliesWriteOptions(t *testing.T) {
	tc := NewTieredCache(nil, time.Minute, time.Minute, WithoutRedis())
	t.Cleanup(func() { _ = tc.Close() })

	before := time.Now()
	loader := func(context.Context) ([]byte, error) { return []byte("loaded"), nil }

	// With redis off, GetOrLoad populates via Set rather than
	// setIfVersion; the options have to survive that route too.
	if _, err := tc.GetOrLoad(context.Background(), "key", loader, LocalTTL(10*time.Second)); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}

	assertLocalTTL(t, tc, "key", before, 10*time.Second)
}

func TestGetOrLoad_Generic_PassesWriteOptionsThrough(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	loader := func(context.Context) (string, error) { return "otmane", nil }

	got, err := GetOrLoad(ctx, tc, JSONMarshaler[string]{}, "user:1", loader, RemoteTTL(7*24*time.Hour))
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if got != "otmane" {
		t.Fatalf("GetOrLoad returned %q, want %q", got, "otmane")
	}

	assertRemoteTTL(t, client, "user:1", 7*24*time.Hour)
}

func TestWithCache_PassesWriteOptionsThrough(t *testing.T) {
	tc, client := newTestTieredCache(t)

	getCountries := WithCache(tc, JSONMarshaler[string]{},
		func(region string) string { return "config:" + region },
		func(_ context.Context, region string) (string, error) { return region + "-data", nil },
		RemoteTTL(7*24*time.Hour),
	)

	if _, err := getCountries(context.Background(), "emea"); err != nil {
		t.Fatalf("getCountries: %v", err)
	}

	assertRemoteTTL(t, client, "config:emea", 7*24*time.Hour)
}

func TestTieredCache_Invalidate_ClearsAKeyWrittenWithOptions(t *testing.T) {
	tc, client := newTestTieredCache(t)
	ctx := context.Background()

	if err := tc.Set(ctx, "key", []byte("value"), RemoteTTL(0)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := tc.Invalidate(ctx, "key"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// A no-expiry write still has to be removable, or RemoteTTL(0) would
	// be a way to strand a key in redis permanently.
	if _, err := client.Get(ctx, "key").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("redis Get after Invalidate: err = %v, want redis.Nil", err)
	}
}
