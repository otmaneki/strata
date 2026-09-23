package strata

import (
	"context"
	"fmt"
)

// GetOrLoad is the generic sibling of Cache's byte-oriented GetOrLoad
// method. It can't be a method itself — Go doesn't allow a method to
// declare its own type parameters — so it takes the Cache to operate on as
// an argument instead, and marshals/unmarshals T via m so callers stop
// hand-rolling json.Marshal/Unmarshal (or any other format) around every
// call site.
//
// It delegates to c.GetOrLoad for the actual fetch, so it gets that
// method's behavior for free: singleflight-deduped loader calls, and
// failing open (returning the loaded value even if caching it afterward
// fails).
//
// A value found in the cache that m can't unmarshal is treated as a real
// error, not a miss to silently retry: unlike a wire-level decode failure
// inside Get (which fails open because the bytes were never valid to begin
// with), reaching here means c.GetOrLoad already returned successfully —
// either a genuine hit, or a value this exact call just marshaled and
// stored — so a failure to read it back means whatever's stored under key
// doesn't match what this Marshaler/T expects, and retrying via loader
// would hit the exact same stale bytes again without evicting them first.
func GetOrLoad[T any](ctx context.Context, c Cache, m Marshaler[T], key string, loader func(context.Context) (T, error)) (T, error) {
	var zero T

	raw, err := c.GetOrLoad(ctx, key, func(ctx context.Context) ([]byte, error) {
		v, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		return m.Marshal(v)
	})
	if err != nil {
		return zero, err
	}

	var v T
	if err := m.Unmarshal(raw, &v); err != nil {
		return zero, fmt.Errorf("unmarshal cached value for key %s: %w", key, err)
	}
	return v, nil
}

// WithCache wraps fn so repeated calls with args that produce the same
// keyFn(args) are served from c instead of calling fn again — a decorator
// built directly on GetOrLoad, for the common case of memoizing a whole
// function once (e.g. at wiring time) instead of calling GetOrLoad inline
// at every call site:
//
//	getUser := strata.WithCache(tc, strata.JSONMarshaler[User]{},
//	    func(id string) string { return "user:" + id },
//	    func(ctx context.Context, id string) (User, error) { return db.GetUser(ctx, id) },
//	)
//	user, err := getUser(ctx, "1234")
func WithCache[Args, T any](c Cache, m Marshaler[T], keyFn func(Args) string, fn func(context.Context, Args) (T, error)) func(context.Context, Args) (T, error) {
	return func(ctx context.Context, args Args) (T, error) {
		return GetOrLoad(ctx, c, m, keyFn(args), func(ctx context.Context) (T, error) {
			return fn(ctx, args)
		})
	}
}
