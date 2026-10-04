package strata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// versionKeyPrefix namespaces the redis key that holds a cache key's
// version counter, kept entirely separate from the value itself. The
// value redis stores under key is always exactly what was given to Set
// or produced by a GetOrLoad loader, byte for byte, nothing prepended or
// wrapped around it. That matters for two reasons: another service
// reading these keys directly sees the same bytes it always would, and
// upgrading to a version of this package that didn't have versioning
// doesn't corrupt whatever's already sitting in redis. An earlier
// version of this file embedded the version as a "<nanoseconds>:<data>"
// prefix on the value itself, which broke both of those: any value
// containing a colon, which is most JSON and plenty of binary payloads,
// got silently truncated on read.
//
// Reserve keys starting with this prefix for this package; don't use it
// yourself.
const versionKeyPrefix = "strata:ver:"

func versionKey(key string) string {
	return versionKeyPrefix + key
}

// setScript atomically writes ARGV[1] under KEYS[1] (the value key) and
// increments KEYS[2] (its version counter), returning the new version.
// KEYS[2] is never deleted and (deliberately) never expires on its own:
// it's redis's own monotonic clock for this key, immune to the clock
// skew a wall-clock timestamp version would be exposed to across
// instances. A plain SET followed by a separate INCR wouldn't be safe
// here: a setIfVersion call for this same key, running concurrently,
// could observe the new value alongside the old version in the gap
// between the two commands, and wrongly conclude nothing has changed.
var setScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])
if tonumber(ARGV[2]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return redis.call('INCR', KEYS[2])
`)

// delScript atomically deletes KEYS[1] (the value key) and increments
// KEYS[2] (its version counter). The increment matters as much as the
// delete: without it, a setIfVersion call already in flight for this
// key, holding a version captured before the delete, would see an
// unchanged version and resurrect the deleted value right back into
// redis.
var delScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
return redis.call('INCR', KEYS[2])
`)

// casScript atomically writes ARGV[1] under KEYS[1] (the value key) and
// increments KEYS[2] (its version counter), but only if KEYS[2]'s current
// value still equals ARGV[2], the version observed before this write was
// decided. Returns 1 if it applied, 0 if it was rejected because the
// version had already moved on. See setIfVersion's doc comment.
var casScript = redis.NewScript(`
local current = redis.call('GET', KEYS[2])
if current == false then
	current = '0'
end
if current ~= ARGV[2] then
	return 0
end
redis.call('SET', KEYS[1], ARGV[1])
if tonumber(ARGV[3]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
redis.call('INCR', KEYS[2])
return 1
`)

// currentVersion returns key's current version counter, or 0 if it has
// none yet (a key that's never been written, or was written before
// versioning existed). GetOrLoad calls this before running its loader,
// to capture the baseline setIfVersion later checks against.
func (tc *TieredCache) currentVersion(ctx context.Context, key string) (int64, error) {
	redisStart := tc.startRedisTimer()
	s, err := tc.redis.Get(ctx, versionKey(key)).Result()
	tc.observeRedisLatency(redisStart)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse version for key %s: %w", key, err)
	}
	return v, nil
}

// setIfVersion writes value under key, encoded through the configured
// Codec the same way Set does, but only if key's version counter still
// equals expectedVersion, the value currentVersion returned before
// GetOrLoad's loader ran. It's GetOrLoad's replacement for a plain Set
// after the loader returns: a loader can be slow, and if a concurrent
// Set, Invalidate, or another GetOrLoad already touched this key while
// this one was still running, blindly overwriting it with the slow
// loader's now-stale result would leave redis permanently wrong, nothing
// would ever notice or correct it.
//
// On a successful write, it also updates the local tier and publishes an
// invalidation, the same as Set. On a rejected write, it touches neither:
// promoting a value redis just refused as stale into this instance's own
// local tier would serve it until localTTL expires, exactly the bug this
// exists to avoid.
func (tc *TieredCache) setIfVersion(ctx context.Context, key string, value []byte, expectedVersion int64) (applied bool, err error) {
	toStore := value
	if tc.codec != nil {
		encoded, err := tc.codec.Encode(value)
		if err != nil {
			tc.stats.encodeErrors.Add(1)
			tc.observer.OnEncodeError(err)
			return false, fmt.Errorf("encode value for key %s: %w", key, err)
		}
		toStore = encoded
	}

	ttlMillis := int64(tc.remoteTTL / time.Millisecond)
	redisStart := tc.startRedisTimer()
	res, err := casScript.Run(ctx, tc.redis, []string{key, versionKey(key)}, toStore, expectedVersion, ttlMillis).Result()
	tc.observeRedisLatency(redisStart)
	if err != nil {
		return false, err
	}

	n, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected cas script result type %T for key %s", res, key)
	}
	applied = n == 1
	if !applied {
		return false, nil
	}

	if tc.localEnabled {
		// A defensive copy, same reasoning as Set: value is the loader's
		// own result, returned to GetOrLoad's caller as well as stored
		// here, and the local tier retains whatever it's given directly.
		localStart := tc.startLocalTimer()
		tc.local.Set(key, bytes.Clone(value), tc.localTTL)
		tc.observeLocalLatency(localStart)
	}

	pubStart := tc.startPubSubTimer()
	pubErr := tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return true, pubErr
}
