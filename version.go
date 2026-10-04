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

// versionKeyPrefix namespaces each key's version counter in a separate
// redis key, so the value stored under the caller's own key is always
// exactly what was given to Set, byte for byte. An earlier version
// embedded the version as a "<nanoseconds>:<data>" prefix on the value
// itself, which silently truncated any value containing a colon, most
// JSON included, on read. Don't use keys starting with this prefix
// yourself.
const versionKeyPrefix = "strata:ver:"

// versionKey wraps key in a redis Cluster hash tag ("{...}"), so it hashes
// to the same slot as the plain, unwrapped value key: Cluster hashes only
// the substring between the first '{' and '}' when present, which here is
// exactly key itself, the same thing a brace-less value key hashes on. The
// two-key Lua scripts below would otherwise hit CROSSSLOT errors against a
// real cluster. This breaks down only if key itself already contains its
// own '{...}' hash tag; there's no way to colocate with an unwrapped value
// key while respecting a caller-chosen hash tag that isn't key's own full
// content.
func versionKey(key string) string {
	return versionKeyPrefix + "{" + key + "}"
}

// setScript atomically writes ARGV[1] under KEYS[1] and increments
// KEYS[2], the version counter, redis's own monotonic clock for this
// key, immune to clock skew between instances. Atomic matters: a plain
// SET then separate INCR would let a concurrent setIfVersion observe the
// new value alongside the old version and wrongly conclude nothing
// changed.
var setScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])
if tonumber(ARGV[2]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return redis.call('INCR', KEYS[2])
`)

// delScript atomically deletes KEYS[1] and increments KEYS[2], the
// version counter. Without the increment, a setIfVersion already in
// flight would see no change and resurrect the deleted value.
var delScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
return redis.call('INCR', KEYS[2])
`)

// casScript writes ARGV[1] under KEYS[1] and increments KEYS[2] only if
// KEYS[2] still equals ARGV[2]; returns 1 if applied, 0 if rejected as
// stale. See setIfVersion.
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

// currentVersion returns key's version counter, or 0 if it has none yet.
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

// setIfVersion is GetOrLoad's replacement for Set after its loader
// returns: it writes value only if key's version still equals
// expectedVersion (from currentVersion, read before the loader ran).
// Otherwise a concurrent write already landed, and applying the loader's
// now-stale result would leave redis wrong with nothing left to correct
// it. A rejected write touches neither the local tier nor pub/sub.
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
		localStart := tc.startLocalTimer()
		tc.local.Set(key, bytes.Clone(value), tc.localTTL)
		tc.observeLocalLatency(localStart)
	}

	pubStart := tc.startPubSubTimer()
	pubErr := tc.redis.Publish(ctx, invalidationPubSubChannel, tc.invalidationPayload(key)).Err()
	tc.observePubSubLatency(pubStart)
	return true, pubErr
}
