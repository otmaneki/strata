// Package strata implements a two-tier cache: an in-process local cache in
// front of redis, with pubsub-driven invalidation so a write on one node
// evicts the stale copy cached on others.
//
// A warm local Get returns the local tier's own backing array, not a
// copy, copying on every local hit would defeat the point of a lock-free
// local tier. Treat it as read-only; everywhere else (Set, GetOrLoad, a
// redis-hit promotion) copies. See Get's doc comment.
package strata

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const invalidationPubSubChannel = "cache:invalidate"

// Default sizing for the local tier every TieredCache starts.
// Override via WithLocalCacheSize and WithLocalCacheEvictInterval.
const (
	defaultLocalCacheMaxSize       = 1_000_000
	defaultLocalCacheEvictInterval = 60 * time.Second
	defaultLoaderTimeout           = 30 * time.Second
)

// TieredCache implements Cache: an in-process local tier in front of
// redis. Either tier can be disabled, but not both; see WithoutLocalCache
// and WithoutRedis.
type TieredCache struct {
	local     *localCache
	redis     redis.UniversalClient
	localTTL  time.Duration
	remoteTTL time.Duration
	sf        singleflight.Group

	localEnabled  bool
	remoteEnabled bool

	observer Observer // defaults to NoopObserver; set via WithObserver

	sub *redis.PubSub

	localCacheMaxSize       int // default sizing for the local tier, see newLocalCache
	localCacheEvictInterval time.Duration
	codec                   Codec
	loaderTimeout           time.Duration

	stats tieredStats

	// Latency histograms, nil unless set via WithRedisLatencyHistogram,
	// WithLocalLatencyHistogram, WithPubSubLatencyHistogram. Publishes get
	// their own since they're fire-and-forget, not a data read/write.
	redisLatency  Histogram
	localLatency  Histogram
	pubsubLatency Histogram

	// instanceID tags this instance's published invalidations, so its own
	// SubscribeInvalidations loop can skip them: redis pub/sub echoes a
	// publish back to the publisher, and without this a node would evict
	// its own freshly written entry on every Set.
	instanceID string

	// invalidator is the invalidation mechanism that the user wants
	// us to use in order to publish the invalidation messages.
	invalidator InvalidatorV2
}

// newInstanceID returns a random id for tagging this instance's published
// invalidations.
func newInstanceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// NewTieredCache builds a TieredCache backed by rc: redis-promoted values
// are kept locally for localTTL, written values are kept in redis for
// remoteTTL. Call Close when done, to stop the local tier's background
// sweeper. rc may be nil if WithoutRedis is used.
//
// Panics if both WithoutLocalCache and WithoutRedis are given: a cache
// that could never store or retrieve anything is a construction mistake,
// not a runtime condition.
func NewTieredCache(rc redis.UniversalClient, localTTL, remoteTTL time.Duration, opts ...Option) *TieredCache {
	tc := &TieredCache{
		redis:                   rc,
		localTTL:                localTTL,
		remoteTTL:               remoteTTL,
		localCacheMaxSize:       defaultLocalCacheMaxSize,
		localCacheEvictInterval: defaultLocalCacheEvictInterval,
		localEnabled:            true,
		remoteEnabled:           true,
		observer:                NoopObserver{},
		instanceID:              newInstanceID(),
		loaderTimeout:           defaultLoaderTimeout,
	}
	for _, opt := range opts {
		opt(tc)
	}
	if !tc.localEnabled && !tc.remoteEnabled {
		//nolint:forbidigo // construction-time misconfiguration, not a runtime condition, same class as regexp.MustCompile
		panic("cache: WithoutLocalCache and WithoutRedis together disable both tiers. TieredCache would never store or retrieve anything")
	}
	if tc.localEnabled {
		tc.local = newLocalCache(tc.localCacheMaxSize, tc.localCacheEvictInterval)
	}
	if tc.invalidator == nil {
		// TODO: Add the default invalidation with pub/sub here.
	}
	return tc
}

// Close stops the local tier's sweeper and any SubscribeInvalidations
// subscription. It does not close the redis client; the caller owns that.
func (tc *TieredCache) Close() error {
	if tc.local != nil {
		tc.local.Close()
	}
	if tc.sub != nil {
		return tc.sub.Close()
	}
	return nil
}

// Stats returns a snapshot of the current hit/miss/error counters. See
// Stats's doc comment for what each field means and how to read them.
func (tc *TieredCache) Stats() Stats {
	return tc.stats.snapshot()
}
