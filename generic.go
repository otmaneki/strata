package strata

import (
	"context"
	"fmt"
)

// GetOrLoad is Cache.GetOrLoad's generic sibling, marshaling/unmarshaling
// T via m instead of taking c as a method (Go methods can't add their
// own type parameters):
//
//	user, err := strata.GetOrLoad(ctx, tc, strata.JSONMarshaler[User]{}, "user:1234",
//	    func(ctx context.Context) (User, error) { return db.GetUser(ctx, 1234) },
//	)
//
// An Unmarshal failure on a cache hit is returned as an error, not
// retried as a miss: the bytes came from c.GetOrLoad succeeding, so
// they're exactly what was last stored under key, and the loader would
// just hit the same bytes again.
//
// opts are passed straight through to c.GetOrLoad, so they override the
// TTLs of the write that caches a miss; see TTL, LocalTTL, RemoteTTL.
func GetOrLoad[T any](ctx context.Context, c Cache, m Marshaler[T], key string, loader func(context.Context) (T, error), opts ...WriteOption) (T, error) {
	var zero T

	raw, err := c.GetOrLoad(ctx, key, func(ctx context.Context) ([]byte, error) {
		v, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		return m.Marshal(v)
	}, opts...)
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
// keyFn(args) are served from c instead of calling fn again. A decorator
// built directly on GetOrLoad, for the common case of memoizing a whole
// function once (e.g. at wiring time) instead of calling GetOrLoad inline
// at every call site:
//
//	getUser := strata.WithCache(tc, strata.JSONMarshaler[User]{},
//	    func(id string) string { return "user:" + id },
//	    func(ctx context.Context, id string) (User, error) { return db.GetUser(ctx, id) },
//	)
//	user, err := getUser(ctx, "1234")
//
// opts are fixed at wrap time and apply to every write the returned
// function makes, which suits the usual case of one TTL policy per
// memoized function:
//
//	getCountries := strata.WithCache(tc, m, keyFn, fetchCountries,
//	    strata.RemoteTTL(7*24*time.Hour))
func WithCache[Args, T any](c Cache, m Marshaler[T], keyFn func(Args) string, fn func(context.Context, Args) (T, error), opts ...WriteOption) func(context.Context, Args) (T, error) {
	return func(ctx context.Context, args Args) (T, error) {
		return GetOrLoad(ctx, c, m, keyFn(args), func(ctx context.Context) (T, error) {
			return fn(ctx, args)
		}, opts...)
	}
}
