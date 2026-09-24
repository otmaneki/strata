package strata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newExampleRedisClient spins up an in-process miniredis instance so these
// examples run with no external services.
func newExampleRedisClient() (*redis.Client, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	return redis.NewClient(&redis.Options{Addr: mr.Addr()}), nil
}

func ExampleTieredCache_Set() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	if err := tc.Set(ctx, "greeting", []byte("hello")); err != nil {
		fmt.Println(err)
		return
	}

	val, ok := tc.Get(ctx, "greeting")
	fmt.Println(string(val), ok)

	// Output:
	// hello true
}

// ExampleTieredCache_GetOrLoad shows the cache-aside pattern: on a miss, the
// loader is invoked once to populate the cache; a second call for the same
// key is served without calling the loader again.
func ExampleTieredCache_GetOrLoad() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	loaderCalls := 0
	loadUser := func(_ context.Context) ([]byte, error) {
		loaderCalls++
		return []byte("otmane"), nil
	}

	first, err := tc.GetOrLoad(ctx, "user:1234", loadUser)
	if err != nil {
		fmt.Println(err)
		return
	}
	second, err := tc.GetOrLoad(ctx, "user:1234", loadUser)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(first))
	fmt.Println(string(second))
	fmt.Println("loader calls:", loaderCalls)

	// Output:
	// otmane
	// otmane
	// loader calls: 1
}

// productFilters is a stand-in for the kind of struct a search/listing
// endpoint builds from request query params — several fields that together
// determine what to query for.
type productFilters struct {
	Category string
	MinPrice int
	InStock  bool
}

// cacheKey turns filters into a deterministic string key: same filters,
// same key, every time. That's the only requirement GetOrLoad has for a
// key — it's written out explicitly here (rather than, say, JSON-marshaling
// the whole struct) so the key format doesn't silently change if a field
// gets renamed or reordered later.
func (f productFilters) cacheKey() string {
	return fmt.Sprintf("products:category=%s:min_price=%d:in_stock=%t", f.Category, f.MinPrice, f.InStock)
}

// buildProductQuery is the "complex processing" step: turning a filter
// struct into a SQL query and its arguments, the way you'd hand them to
// database/sql's QueryContext. It just returns the query text here so the
// example stays runnable without a real database.
func buildProductQuery(f productFilters) (query string, args []any) {
	query = "SELECT id, name, price FROM products WHERE category = ? AND price >= ?"
	args = []any{f.Category, f.MinPrice}
	if f.InStock {
		query += " AND in_stock = true"
	}
	return query, args
}

// ExampleTieredCache_GetOrLoad_filters shows GetOrLoad's loader doing real
// work instead of a trivial lookup: it's an ordinary closure, so it can
// capture whatever it needs from the enclosing scope — here, a filters
// struct and (in a real program) a *sql.DB — build a query from it, and
// execute it. GetOrLoad doesn't know or care what's inside the loader; all
// it needs is a key that's deterministic for the same filters, which is
// exactly what filters.cacheKey() gives it. A second call with the same
// filters is served from cache without building or running the query
// again.
//
// If you want a reusable, typed function instead of calling GetOrLoad
// inline at every call site — e.g. "listProducts(ctx, filters)" — wrap this
// same loader with WithCache instead: it takes the filters struct as its
// Args type directly, so keyFn and the query-building logic look the same,
// just moved into WithCache's wiring instead of a call site.
func ExampleTieredCache_GetOrLoad_filters() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	filters := productFilters{Category: "shoes", MinPrice: 20, InStock: true}

	queriesRun := 0
	loadProducts := func(ctx context.Context) ([]byte, error) {
		queriesRun++
		query, args := buildProductQuery(filters)
		fmt.Println("SQL:", query, args)
		// A real implementation executes the query here, e.g.:
		//   rows, err := db.QueryContext(ctx, query, args...)
		// then scans rows into a slice and marshals that. This example
		// fakes the result to stay runnable without a database.
		return json.Marshal([]string{"sneaker", "boot"})
	}

	first, err := tc.GetOrLoad(ctx, filters.cacheKey(), loadProducts)
	if err != nil {
		fmt.Println(err)
		return
	}
	second, err := tc.GetOrLoad(ctx, filters.cacheKey(), loadProducts)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(first))
	fmt.Println(string(second))
	fmt.Println("queries run:", queriesRun)

	// Output:
	// SQL: SELECT id, name, price FROM products WHERE category = ? AND price >= ? AND in_stock = true [shoes 20]
	// ["sneaker","boot"]
	// ["sneaker","boot"]
	// queries run: 1
}

// exampleUser is the payload used by ExampleGetOrLoad and ExampleWithCache
// below, marshaled through JSONMarshaler.
type exampleUser struct {
	ID   string
	Name string
}

// ExampleGetOrLoad shows the generic, typed sibling of TieredCache's
// []byte-oriented GetOrLoad: it marshals/unmarshals T through the given
// Marshaler, so callers stop hand-rolling json.Marshal/Unmarshal around
// every call site. Like the byte-oriented GetOrLoad, a second call for the
// same key is served from cache without calling the loader again.
func ExampleGetOrLoad() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	loaderCalls := 0
	loadUser := func(context.Context) (exampleUser, error) {
		loaderCalls++
		return exampleUser{ID: "1234", Name: "otmane"}, nil
	}

	first, err := GetOrLoad(ctx, tc, JSONMarshaler[exampleUser]{}, "user:1234", loadUser)
	if err != nil {
		fmt.Println(err)
		return
	}
	second, err := GetOrLoad(ctx, tc, JSONMarshaler[exampleUser]{}, "user:1234", loadUser)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(first.Name)
	fmt.Println(second.Name)
	fmt.Println("loader calls:", loaderCalls)

	// Output:
	// otmane
	// otmane
	// loader calls: 1
}

// ExampleWithCache shows memoizing a whole function once, at wiring time,
// instead of calling GetOrLoad inline at every call site: getUser below is
// an ordinary func(context.Context, string) (exampleUser, error) that
// happens to be cache-backed, so it drops straight into code that already
// expects that shape.
func ExampleWithCache() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute)
	defer tc.Close() //nolint:errcheck // example cleanup

	loaderCalls := 0
	getUser := WithCache(tc, JSONMarshaler[exampleUser]{},
		func(id string) string { return "user:" + id },
		func(_ context.Context, id string) (exampleUser, error) {
			loaderCalls++
			return exampleUser{ID: id, Name: "otmane"}, nil
		},
	)

	ctx := context.Background()
	first, err := getUser(ctx, "1234")
	if err != nil {
		fmt.Println(err)
		return
	}
	second, err := getUser(ctx, "1234")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(first.Name)
	fmt.Println(second.Name)
	fmt.Println("loader calls:", loaderCalls)

	// Output:
	// otmane
	// otmane
	// loader calls: 1
}

// ExampleWithLocalCacheSize shows bounding the local tier's size. The
// default is 1,000,000 entries; here it's capped at 2 to demonstrate the
// bound taking effect after just 3 writes.
func ExampleWithLocalCacheSize() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(
		client, time.Minute, time.Minute,
		WithLocalCacheSize(2),
		WithLocalCacheEvictInterval(time.Hour),
	)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	for i, key := range []string{"a", "b", "c"} {
		if err := tc.Set(ctx, key, fmt.Appendf(nil, "%d", i)); err != nil {
			fmt.Println(err)
			return
		}
	}

	fmt.Println(tc.local.Len() <= 2)

	// Output:
	// true
}

// ExampleWithoutRedis shows using only the in-process local tier, with no
// redis involved at all. The redis client argument to NewTieredCache can be
// nil in this mode — it's never dialed — which makes this the cheapest way
// to get TieredCache's API (including GetOrLoad's singleflight dedup)
// without a redis dependency, at the cost of no cross-instance consistency.
func ExampleWithoutRedis() {
	tc := NewTieredCache(nil, time.Minute, time.Minute, WithoutRedis())
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	if err := tc.Set(ctx, "greeting", []byte("hello")); err != nil {
		fmt.Println(err)
		return
	}

	val, ok := tc.Get(ctx, "greeting")
	fmt.Println(string(val), ok)

	// Output:
	// hello true
}

// ExampleWithoutLocalCache shows using only the redis tier, with no
// in-process local cache. Every Get and Set goes straight to redis, and
// nothing is ever held in per-instance memory — the trade to reach for when
// even localTTL of staleness, or per-instance memory use, is unacceptable.
func ExampleWithoutLocalCache() {
	client, err := newExampleRedisClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close() //nolint:errcheck // example cleanup

	tc := NewTieredCache(client, time.Minute, time.Minute, WithoutLocalCache())
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx := context.Background()
	if err := tc.Set(ctx, "greeting", []byte("hello")); err != nil {
		fmt.Println(err)
		return
	}

	val, ok := tc.Get(ctx, "greeting")
	fmt.Println(string(val), ok)

	// Output:
	// hello true
}

// exampleObserver embeds NoopObserver so it only has to implement the one
// event it cares about — the pattern Observer's doc comment recommends for
// your own Observer implementations.
type exampleObserver struct {
	NoopObserver
	onRedisError func(error)
}

func (o exampleObserver) OnRedisError(err error) {
	if o.onRedisError != nil {
		o.onRedisError(err)
	}
}

// ExampleWithObserver shows observing redis failures that Get would
// otherwise report only as a plain cache miss.
func ExampleWithObserver() {
	// Nothing listens on this port, so every call fails outright.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close() //nolint:errcheck // example cleanup

	var lastErr error
	tc := NewTieredCache(
		client, time.Minute, time.Minute,
		WithObserver(exampleObserver{onRedisError: func(err error) { lastErr = err }}),
	)
	defer tc.Close() //nolint:errcheck // example cleanup

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, ok := tc.Get(ctx, "key")
	fmt.Println("hit:", ok)
	fmt.Println("error observed:", lastErr != nil)

	// Output:
	// hit: false
	// error observed: true
}
