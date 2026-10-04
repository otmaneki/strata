package strata

import (
	"bytes"
	"time"
)

// WriteOption configures a single write: a Set call, or the cache write
// GetOrLoad performs once its loader returns. Distinct from Option,
// which configures a TieredCache once, at construction, and applies to
// every write it ever makes.
type WriteOption func(*writeOptions)

// writeOptions collects one write's TTL overrides before any of them are
// folded onto the cache's defaults. The fields are pointers so that
// "never specified" stays distinguishable from "specified as zero",
// which is itself meaningful: see RemoteTTL and LocalTTL.
type writeOptions struct {
	ttl       *time.Duration // both tiers, from TTL
	localTTL  *time.Duration
	remoteTTL *time.Duration
}

// TTL overrides both tiers' TTL for this write. LocalTTL and RemoteTTL
// win over it for their own tier, in whichever order the options are
// given, so
//
//	tc.Set(ctx, key, val, strata.TTL(30*time.Minute), strata.LocalTTL(time.Minute))
//
// keeps the value for 30 minutes in redis and one minute locally.
func TTL(d time.Duration) WriteOption {
	return func(o *writeOptions) { o.ttl = &d }
}

// LocalTTL overrides the local tier's TTL for this write, leaving the
// redis tier on the cache's default (or on TTL, if that's given too).
//
// A duration <= 0 means "don't keep this locally": the write deletes any
// entry already cached under the key instead of storing one that's
// expired on arrival. Every read then goes to redis, for this key alone,
// the per-write counterpart to WithoutLocalCache.
func LocalTTL(d time.Duration) WriteOption {
	return func(o *writeOptions) { o.localTTL = &d }
}

// RemoteTTL overrides the redis tier's TTL for this write, leaving the
// local tier on the cache's default (or on TTL, if that's given too).
//
// A duration <= 0 writes the key with no expiry at all, matching what
// NewTieredCache does with a non-positive remoteTTL: redis holds the
// value until something overwrites or invalidates it.
func RemoteTTL(d time.Duration) WriteOption {
	return func(o *writeOptions) { o.remoteTTL = &d }
}

// resolveTTLs folds opts onto the TTLs NewTieredCache was given.
// Precedence runs specific over general over default: LocalTTL/RemoteTTL
// beat TTL, which beats the cache's own. Collecting the options into a
// struct first, rather than letting each one write straight through to a
// duration, is what makes that independent of the order they arrive in,
// TTL after LocalTTL resolves the same as TTL before it.
//
// Whatever it resolves to, the local TTL is capped by the remote one,
// see capLocalTTL. That applies to the cache's own defaults too, not
// just to opts: NewTieredCache accepts a localTTL longer than its
// remoteTTL, and that combination has the same problem.
func (tc *TieredCache) resolveTTLs(opts []WriteOption) (local, remote time.Duration) {
	local, remote = tc.localTTL, tc.remoteTTL
	if len(opts) == 0 {
		return capLocalTTL(local, remote), remote
	}

	var o writeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.ttl != nil {
		local, remote = *o.ttl, *o.ttl
	}
	if o.localTTL != nil {
		local = *o.localTTL
	}
	if o.remoteTTL != nil {
		remote = *o.remoteTTL
	}
	return capLocalTTL(local, remote), remote
}

// capLocalTTL holds the local tier to the remote tier's lifetime: a
// cached copy must never outlive the value it was cached from.
//
// Nothing publishes an invalidation when a redis key merely expires, so
// a local copy that outlives it keeps being served with no source of
// truth left to correct it, and no event that would ever evict it early.
// A remote TTL of 0 or less means the key never expires in redis, so
// there's nothing to cap against.
//
// This is deliberately silent rather than an error: local > remote is
// almost always a mistake, and the one arguably coherent reading of it,
// "accept more staleness locally than redis keeps", is better expressed
// by raising the remote TTL.
func capLocalTTL(local, remote time.Duration) time.Duration {
	if remote > 0 && local > remote {
		return remote
	}
	return local
}

// ttlMilliseconds converts d into the whole milliseconds the Lua scripts
// take as their expiry argument.
//
// It rounds up, so every positive duration stays positive. Truncating
// turns anything under a millisecond into 0, which the scripts read as
// "no expiry": asking for a 500µs lifetime and getting an immortal key
// is the one direction a cache must never fail by accident. A
// non-positive d still maps to 0, which is how RemoteTTL(0) asks for no
// expiry on purpose.
func ttlMilliseconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// setLocal writes value into the local tier under the resolved per-write
// TTL, timing the call. A non-positive ttl deletes the key instead of
// storing it: whatever is cached under it is stale the moment this write
// lands, and leaving it would serve the old value locally while redis
// serves the new one.
//
// value is copied before it's stored, so callers can hand over a buffer
// they intend to reuse. The copy happens here rather than at the call
// site so it's skipped entirely on the paths that never store anything:
// a disabled local tier, or a non-positive ttl.
func (tc *TieredCache) setLocal(key string, value []byte, ttl time.Duration) {
	if !tc.localEnabled {
		return
	}
	localStart := tc.startLocalTimer()
	if ttl > 0 {
		tc.local.Set(key, bytes.Clone(value), ttl)
	} else {
		tc.local.Delete(key)
	}
	tc.observeLocalLatency(localStart)
}
