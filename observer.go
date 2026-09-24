package strata

// Observer receives notifications about failures inside a TieredCache. All
// methods must be safe to call concurrently and should return quickly.
// They're invoked inline on the request path, not on a separate goroutine.
// Wire an Observer up to your metrics/logger; without one (the default), a
// TieredCache uses NoopObserver and every event is silently dropped.
//
// Embed NoopObserver in your own type to satisfy Observer without
// implementing every method. A method added to this interface later then
// defaults to a no-op for existing implementers instead of breaking them.
type Observer interface {
	// OnRedisError is called when a redis call inside Get fails for a
	// reason other than a genuine cache miss (redis.Nil). Get still
	// reports these to its caller as a plain miss (failing open rather
	// than surfacing a redis outage as an error), so without this hook a
	// redis outage looks identical to a 100% cold cache.
	OnRedisError(err error)

	// OnEncodeError is called when the configured Codec fails to encode a
	// value in Set.
	OnEncodeError(err error)

	// OnDecodeError is called when the configured Codec fails to decode a
	// value read from redis in Get. A decode failure is reported to the
	// caller as a plain miss, so without this hook it's indistinguishable
	// from the key simply not existing.
	OnDecodeError(err error)

	// OnSetError is called when GetOrLoad's internal Set call fails after
	// a successful loader call. GetOrLoad still returns the loaded value
	// in that case (failing open, same principle as Get): the loader
	// already did the real work, and a cache-population failure shouldn't
	// fail the caller's request on top of it.
	OnSetError(err error)
}

// NoopObserver implements Observer with methods that do nothing. It's the
// default Observer for a TieredCache, and it's meant to be embedded in your
// own observer type when you only care about some events, see Observer's
// doc comment.
type NoopObserver struct{}

// OnRedisError does nothing; see Observer.OnRedisError.
func (NoopObserver) OnRedisError(error) {}

// OnEncodeError does nothing; see Observer.OnEncodeError.
func (NoopObserver) OnEncodeError(error) {}

// OnDecodeError does nothing; see Observer.OnDecodeError.
func (NoopObserver) OnDecodeError(error) {}

// OnSetError does nothing; see Observer.OnSetError.
func (NoopObserver) OnSetError(error) {}

var _ Observer = NoopObserver{}

// Compile-time proof that embedding NoopObserver alone satisfies Observer,
// the whole point of providing it.
var _ Observer = struct{ NoopObserver }{}
