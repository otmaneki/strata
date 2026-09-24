package strata

import "encoding/json"

// Marshaler converts a T to and from the []byte a Cache actually stores.
// It's the boundary the generic GetOrLoad and WithCache use so callers
// never hand-roll json.Marshal/Unmarshal (or any other format) around a
// cache call themselves, see JSONMarshaler for the common case.
//
// This is a different concern from Codec: Codec transforms already-encoded
// bytes for the wire (e.g. compression) and only ever applies to the redis
// tier inside TieredCache. Marshaler is how a domain value becomes bytes in
// the first place, and it applies before Codec ever sees anything.
type Marshaler[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte, v *T) error
}

// JSONMarshaler is a Marshaler backed by encoding/json, the default most
// callers want:
//
//	strata.GetOrLoad(ctx, tc, strata.JSONMarshaler[User]{}, key, loader)
type JSONMarshaler[T any] struct{}

// Marshal is the default json/Marshaller.
func (JSONMarshaler[T]) Marshal(v T) ([]byte, error) { return json.Marshal(v) }

// Unmarshal is the default json/Unmarshaller.
func (JSONMarshaler[T]) Unmarshal(data []byte, v *T) error { return json.Unmarshal(data, v) }

var _ Marshaler[struct{}] = JSONMarshaler[struct{}]{}
