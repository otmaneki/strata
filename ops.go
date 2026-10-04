package strata

import (
	"bytes"
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

	redisStart := tc.startRedisTimer()
	val, err := tc.redis.Get(ctx, key).Bytes()
	tc.observeRedisLatency(redisStart)
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

		if tc.localEnabled {
			// Copy before storing: decoded is also returned below, and
			// a caller mutating it mustn't reach into the local tier.
			localStart := tc.startLocalTimer()
			tc.local.Set(key, bytes.Clone(decoded), tc.localTTL)
			tc.observeLocalLatency(localStart)
		}
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

	ttlMillis := int64(remoteTTL / time.Millisecond)
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
