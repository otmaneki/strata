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
func (tc *TieredCache) resolveTTLs(opts []WriteOption) (local, remote time.Duration) {
	local, remote = tc.localTTL, tc.remoteTTL
	if len(opts) == 0 {
		return local, remote
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
	return local, remote
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
