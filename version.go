package strata

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// casScript atomically writes a versioned value under KEYS[1] only if
// ARGV[1] (the new version, nanoseconds) is strictly newer than whatever
// version is already stored there, or nothing is stored yet. It returns 1
// if the write applied, 0 if it was rejected as stale. ARGV[2] is the
// value to store (already encoded, if a Codec is set), ARGV[3] is the TTL
// in milliseconds, or 0 for no expiry.
//
// See setIfNewer's doc comment for why this exists.
var casScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
local currentVer = -1
if current then
	local sep = string.find(current, ':')
	if sep then
		currentVer = tonumber(string.sub(current, 1, sep - 1))
	end
end
local newVer = tonumber(ARGV[1])
if currentVer ~= nil and newVer <= currentVer then
	return 0
end
local stored = ARGV[1] .. ':' .. ARGV[2]
if tonumber(ARGV[3]) > 0 then
	redis.call('SET', KEYS[1], stored, 'PX', ARGV[3])
else
	redis.call('SET', KEYS[1], stored)
end
return 1
`)

// versionedPayload prepends version, as decimal nanoseconds followed by a
// colon, to data. stripVersion reverses it. Every value TieredCache writes
// to redis is wrapped this way, so a later setIfNewer call can tell
// whether the value it's about to write is actually newer than whatever's
// already there.
func versionedPayload(version int64, data []byte) []byte {
	prefix := strconv.FormatInt(version, 10) + ":"
	out := make([]byte, 0, len(prefix)+len(data))
	out = append(out, prefix...)
	out = append(out, data...)
	return out
}

// stripVersion removes a versionedPayload's prefix, returning the
// original data. If stored has no colon at all, it's returned unchanged:
// that's a value written before this scheme existed, or data seeded some
// other way, and dropping it instead of returning it would be worse than
// skipping the version check for it.
func stripVersion(stored []byte) []byte {
	i := bytes.IndexByte(stored, ':')
	if i < 0 {
		return stored
	}
	return stored[i+1:]
}

// setIfNewer writes value under key, encoded through the configured Codec
// the same way Set does, but only if version is strictly newer than
// whatever version redis already has for key. It's GetOrLoad's
// replacement for a plain Set after the loader returns: a loader can be
// slow, and if a concurrent Set or another GetOrLoad already wrote a
// fresher value for the same key while this one was still running,
// blindly overwriting it with the slow loader's now-stale result would
// leave redis permanently wrong, nothing would ever notice or correct it.
// version should be a timestamp taken before the loader ran, not after,
// so a slow loader's result is correctly treated as old even if it
// finishes after the newer write.
//
// On a successful write, it also updates the local tier and publishes an
// invalidation, the same as Set. On a rejected write, it touches neither:
// promoting a value redis just refused as stale into this instance's own
// local tier would serve it until localTTL expires, exactly the bug this
// exists to avoid.
func (tc *TieredCache) setIfNewer(ctx context.Context, key string, value []byte, version int64) (applied bool, err error) {
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
	res, err := casScript.Run(ctx, tc.redis, []string{key}, version, toStore, ttlMillis).Result()
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
