package strata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// invalidationPayload tags key's pubsub message with this instance's id,
// so SubscribeInvalidations can skip the instance's own publishes.
func (tc *TieredCache) invalidationPayload(key string) string {
	return tc.instanceID + ":" + key
}

// promotionTTL is how long a redis hit should be kept in the local tier:
// the cache's configured localTTL, capped by whatever lifetime the key
// has left in redis.
//
// The cap is what makes a per-write RemoteTTL mean anything on a node
// that didn't perform the write. Per-write TTLs live only in the writer's
// call, so without this a key written with RemoteTTL(30*time.Second)
// would be promoted for the full default localTTL by every node that
// reads it, outliving the redis copy it came from. Nothing publishes an
// invalidation when a key merely expires, so that stale local copy would
// have no source of truth left to correct it.
//
// pttl is redis's PTTL reply as go-redis reports it: -1 for a key with no
// expiry, and so nothing to cap against, -2 for one that's already gone.
// An error reading it falls back to the configured localTTL, which is the
// behavior this had before the cap existed.
func (tc *TieredCache) promotionTTL(pttl time.Duration, err error) time.Duration {
	switch {
	case err != nil:
		return tc.localTTL
	case pttl == -2:
		// Expired between the GET and the PTTL. The value is still
		// returned to the caller, it was real when GET ran, but caching
		// it now would outlive redis by the whole localTTL. A
		// non-positive TTL makes setLocal drop the key instead.
		return 0
	case pttl > 0 && pttl < tc.localTTL:
		return pttl
	default:
		return tc.localTTL
	}
}

// Get checks the local tier first, then redis, promoting a redis hit
// into the local tier. A warm local hit returns the local tier's own
// backing array, not a copy; everything else here returns a fresh one.
func (tc *TieredCache) Get(ctx context.Context, key string) ([]byte, bool) {
	if tc.localEnabled {
		localStart := tc.startLocalTimer()
		val, ok := tc.local.Get(key)
		tc.observeLocalLatency(localStart)
		if ok {
			// Guard the assertion rather than trust it: TieredCache only
			// ever stores []byte here, but a panic on a violated
			// invariant is worse than a spurious miss.
			if b, ok := val.([]byte); ok {
				tc.stats.localHits.Add(1)
				return b, true
			}
			return nil, false
		}
		tc.stats.localMisses.Add(1)
	}

	if !tc.remoteEnabled {
		return nil, false
	}

	// GET and PTTL together, pipelined into one round trip: the promotion
	// below needs the key's remaining lifetime to avoid caching it
	// locally for longer than redis will keep it. See promotionTTL.
	redisStart := tc.startRedisTimer()
	pipe := tc.redis.Pipeline()
	getCmd := pipe.Get(ctx, key)
	pttlCmd := pipe.PTTL(ctx, key)
	// Exec's own error is just the first command's, redis.Nil on a plain
	// miss included, so the commands are inspected individually instead.
	_, _ = pipe.Exec(ctx)
	tc.observeRedisLatency(redisStart)

	val, err := getCmd.Bytes()
	switch {
	case err == nil:
		tc.stats.redisHits.Add(1)
		decoded := val
		if tc.codec != nil {
			decoded, err = tc.codec.Decode(val)
			if err != nil {
				// Fail open, same as a redis error: don't promote
				// undecodable bytes into the local tier.
				tc.stats.decodeErrors.Add(1)
				tc.observer.OnDecodeError(err)
				return nil, false
			}
		}

		// setLocal copies decoded before storing it: decoded is also
		// returned below, and a caller mutating it mustn't reach into
		// the local tier.
		tc.setLocal(key, decoded, tc.promotionTTL(pttlCmd.Result()))
		return decoded, true
	case errors.Is(err, redis.Nil):
		tc.stats.redisMisses.Add(1)
		return nil, false
	default:
		// Fail open: report a miss, not an error, but let the Observer
		// know, or a redis outage would look identical to a cold cache.
		tc.stats.redisErrors.Add(1)
		tc.observer.OnRedisError(err)
		return nil, false
	}
}

// Set writes value to both tiers (copying it, safe to reuse value
// afterward), bumps key's version counter in redis for setIfVersion, and
// publishes an invalidation so other instances drop their stale local
// copy. It always applies, unconditionally; see setIfVersion for the
// conditional write GetOrLoad needs instead.
//
// Both tiers use the TTLs NewTieredCache was given unless opts override
// them for this one write:
//
//	tc.Set(ctx, "session:abc", data)                              // both defaults
//	tc.Set(ctx, "config:countries", data, strata.RemoteTTL(7*24*time.Hour))
//	tc.Set(ctx, "session:abc", data, strata.TTL(30*time.Minute), strata.LocalTTL(time.Minute))
//
// See TTL, LocalTTL, and RemoteTTL.
func (tc *TieredCache) Set(ctx context.Context, key string, value []byte, opts ...WriteOption) error {
	localTTL, remoteTTL := tc.resolveTTLs(opts)
	tc.setLocal(key, value, localTTL)

	if !tc.remoteEnabled {
		return nil
	}

	toStore := value
	if tc.codec != nil {
		encoded, err := tc.codec.Encode(value)
		if err != nil {
			tc.stats.encodeErrors.Add(1)
			tc.observer.OnEncodeError(err)
			return fmt.Errorf("encode value for key %s: %w", key, err)
		}
		toStore = encoded
	}

	ttlMillis := ttlMilliseconds(remoteTTL)
	redisStart := tc.startRedisTimer()
	err := setScript.Run(ctx, tc.redis, []string{key, versionKey(key)}, toStore, ttlMillis).Err()
	tc.observeRedisLatency(redisStart)
	if err != nil {
		return err
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// Invalidate deletes key and publishes an invalidation for other
// instances. The redis delete also bumps key's version counter,
// atomically: a setIfVersion call already in flight for this key must
// see that something changed, or it would resurrect the value this just
// deleted.
func (tc *TieredCache) Invalidate(ctx context.Context, key string) error {
	if tc.localEnabled {
		localStart := tc.startLocalTimer()
		tc.local.Delete(key)
		tc.observeLocalLatency(localStart)
	}

	if !tc.remoteEnabled {
		return nil
	}

	delStart := tc.startRedisTimer()
	err := delScript.Run(ctx, tc.redis, []string{key, versionKey(key)}).Err()
	tc.observeRedisLatency(delStart)
	if err != nil {
		return fmt.Errorf("delete %s from redis: %w", key, err)
	}

	pubStart := tc.startPubSubTimer()
	err = tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return err
}

// SubscribeInvalidations listens for Set/Invalidate's published
// invalidations and evicts the affected key locally. Call at most once;
// Close stops it. No-op if either tier is disabled.
func (tc *TieredCache) SubscribeInvalidations(ctx context.Context) {
	if !tc.localEnabled || !tc.remoteEnabled {
		return
	}
	tc.sub = tc.redis.Subscribe(ctx, invalidationPubSubChannel)
	go func() {
		for msg := range tc.sub.Channel() {
			origin, key, found := strings.Cut(msg.Payload, ":")
			if !found {
				key = origin // no instance id prefix: treat it all as the key
			} else if origin == tc.instanceID {
				continue // our own publish echoed back; already applied locally
			}
			tc.local.Delete(key)
		}
	}()
}
