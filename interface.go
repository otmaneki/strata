package strata

import (
	"context"
	"io"
)

// Reader is the read side of Cache: looking up a value by key.
type Reader interface {
	// Get looks up key, returning its value and whether it was found. A
	// "not found" result must cover both a genuine miss and any backend
	// failure — callers can't tell the two apart from the return value
	// alone.
	Get(ctx context.Context, key string) ([]byte, bool)
}

// Writer is the write side of Cache: storing a value under a key.
type Writer interface {
	// Set writes value under key.
	Set(ctx context.Context, key string, value []byte) error
}

// ReadWriter is Reader and Writer combined — the shape most request-path
// code actually needs: look values up, store them, nothing else.
type ReadWriter interface {
	Reader
	Writer
}

// Loader is the cache-aside side of Cache: fetching a value and populating
// it on a miss.
type Loader interface {
	// GetOrLoad returns the cached value for key, invoking loader to
	// populate the cache on a miss. Concurrent calls for the same missing
	// key should share one loader invocation rather than each calling it.
	GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error)
}

// Invalidator is the cross-instance side of Cache: removing a key and
// propagating that removal to other instances.
type Invalidator interface {
	// Invalidate removes key and, where the implementation supports
	// cross-instance invalidation, notifies other instances to do the same.
	Invalidate(ctx context.Context, key string) error

	// SubscribeInvalidations starts listening for invalidations published
	// by other instances' Invalidate calls. A no-op implementation is
	// valid for a cache with no cross-instance invalidation to subscribe to.
	SubscribeInvalidations(ctx context.Context)
}

// Cache is the full method set TieredCache implements: Reader, Writer,
// Loader, and Invalidator, plus io.Closer for releasing owned resources.
//
// Depend on the smallest of these your code actually calls, not
// necessarily the whole Cache — a request handler that only reads and
// writes needs ReadWriter, not Invalidate/SubscribeInvalidations/Close, and
// its test mocks shrink to match. Reach for Cache itself where code really
// does span the full lifecycle (e.g. wherever constructs a TieredCache and
// owns Close), or when handing the cache to code you don't control the
// interface of.
//
// TieredCache is the only production implementation; see its doc comments
// for the full behavior contract (e.g. how Get treats a redis error versus
// a genuine miss) — a test double should honor the same contract for the
// methods it exercises.
type Cache interface {
	Reader
	Writer
	Loader
	Invalidator
	io.Closer
}

// Compile-time guard: if TieredCache's method set ever drifts from Cache,
// this line fails to build instead of the mismatch surfacing later as a
// runtime type-assertion failure somewhere a caller uses Cache.
var _ Cache = (*TieredCache)(nil)
