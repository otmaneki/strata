package strata

import (
	"context"
	"io"
)

// Reader is the read side of Cache: looking up a value by key.
type Reader interface {
	// Get looks up key. The bool reports whether it was found; a backend
	// failure looks the same as a genuine miss to the caller. Whether the
	// returned []byte is a copy is implementation-defined, check the
	// concrete type before mutating it.
	Get(ctx context.Context, key string) ([]byte, bool)
}

// Writer is the write side of Cache: storing a value under a key.
type Writer interface {
	// Set writes value under key. Whether value is copied or retained is
	// implementation-defined, check the concrete type before reusing or
	// mutating it afterward.
	Set(ctx context.Context, key string, value []byte) error
}

// ReadWriter is Reader and Writer combined, the shape most request-path
// code needs: look values up, store them, nothing else.
type ReadWriter interface {
	Reader
	Writer
}

// Loader is the cache-aside side of Cache: fetching a value and
// populating it on a miss.
type Loader interface {
	// GetOrLoad returns the cached value for key, calling loader to
	// populate it on a miss. Concurrent misses for the same key should
	// share one loader call, not each trigger their own:
	//
	//	data, err := c.GetOrLoad(ctx, "user:1234", func(ctx context.Context) ([]byte, error) {
	//	    u, err := db.GetUser(ctx, 1234)
	//	    if err != nil {
	//	        return nil, err
	//	    }
	//	    return json.Marshal(u)
	//	})
	GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error)
}

// Invalidator is the cross-instance side of Cache: removing a key and
// propagating that removal to other instances.
type Invalidator interface {
	// Invalidate removes key and, where supported, notifies other
	// instances to do the same.
	Invalidate(ctx context.Context, key string) error

	// SubscribeInvalidations listens for invalidations from other
	// instances' Invalidate calls. A no-op implementation is valid if
	// there's nothing to subscribe to.
	SubscribeInvalidations(ctx context.Context)
}

// Cache is the full method set TieredCache implements. Depend on the
// smallest of Reader, Writer, ReadWriter, Loader, or Invalidator your code
// actually needs instead, so test mocks shrink to match.
type Cache interface {
	Reader
	Writer
	Loader
	Invalidator
	io.Closer
}

// Compile-time guard: if TieredCache's method set ever drifts from Cache,
// this fails to build instead of surfacing later as a runtime panic.
var _ Cache = (*TieredCache)(nil)
