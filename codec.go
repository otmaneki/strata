package strata

// Codec interface provides the encoding and decoding for the caching
// and this is only for the remote storage nothing touches the local storage
// because it will trash our numbers and there is no need for it.
type Codec interface {
	Encode(value []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}
