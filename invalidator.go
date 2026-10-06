package strata

import (
	"context"
	"uuid"
)

// Invalidation is the invalidation event that we will broadcast to the rest of the nodes.
type Invalidation struct {
	Key     string
	Version uint64
	NodeID  uuid.UUID
}

// InvalidatorV2 is the new interface we will be using for our invalidation
// Mainly we will let it up to the user to either provide us with an invalidation
// mechanism that implements this interface, it can be Kafka/RPC/sockets/redis-streams/etc...
// otherwise we will fallback to our redis pub-sub.
type InvalidatorV2 interface {
	Invalidate(ctx context.Context, event Invalidation) error

	// SubscribeInvalidations listens for invalidations from other
	// instances' Invalidate calls. A no-op implementation is valid if
	// there's nothing to subscribe to.
	Subscribe(ctx context.Context)
}
