# TODOs for strata, the 2 tier-caching library.

# Strata — TODO & Project Direction

> **Distributed two-tier cache / coherence project**

## 1. What Strata is becoming

Strata started as a two-tier cache, but the design is evolving into a cache with a **configurable distributed coherence mechanism**.

The important problem is not simply caching data locally and in Redis. It is allowing independent application nodes to maintain a useful notion of freshness **without putting a network round trip on every local-cache read**.

Core model:

```text
L1: in-process cache
L2: Redis / shared state
+
versioning
+
stale-write prevention
+
version-aware invalidation
+
invalidation over Redis Pub/Sub or Redis Streams
```

---

# 2. TODO — Version-aware invalidation

### Local cache state

- [ ] Add the version to each L1 cache entry.

For example:

```go
type entry struct {
    value     []byte
    version   uint64
    expiresAt time.Time
}
```

### Version-bearing invalidation

- [ ] Change invalidation events from key-only to **key + version**.

```go
type Invalidation struct {
    Key     string
    Version uint64
}
```

The meaning should be:

> The authoritative version of this key has advanced to at least `Version`.

### Monotonicity

- [ ] Make invalidation idempotent.
- [ ] Ensure local knowledge of a key's version can never move backwards.

Example:

```text
local version = 41

receive V43 → known version becomes 43
receive V42 → no-op
receive V43 → no-op
receive V45 → known version becomes 45
```

Conceptually:

```text
knownVersion[key] = max(knownVersion[key], event.Version)
```

In practice `knownVersion[key]` is just the L1 entry's `version`, as long as invalidations leave a tombstone instead of deleting the entry (see **Tombstones** below). Deleting the entry throws that knowledge away.

### No Redis RTT on L1 hits

- [ ] Do **not** check Redis's version on every L1 read.

The whole point of L1 is:

```text
request
  ↓
L1 hit
  ↓
return immediately
```

Freshness information should arrive asynchronously through the invalidation mechanism.

### Originating writer

- [ ] Write to Redis **first**, then update L1 with the version the script returned. Today `Set` writes L1 before Redis.

Desired flow:

```text
Node A receives SET
       ↓
one Lua script: Redis value + version updated, (key, version) event emitted
       ↓
Node A L1 updated, only if the version is newer than what's there
       ↓
other nodes converge
```

The writer should **not** need to wait for invalidation propagation before serving the new value locally.

- [ ] Make `casScript` return the new version on success, and 0 when rejected (versions start at 1 after `INCR`, so 0 is free to mean "rejected").
- [ ] If the Redis call errors, don't install anything locally: delete the local key instead. On a timeout you can't tell whether the script ran, so the old local value may already be stale.
- [ ] `WithoutRedis`: there's no version to get. Store with version 0 and keep last-write-wins, since L1 is the only copy.

Reordering alone doesn't fix concurrent writers on the same node. They can reach L1 in a different order than they reached Redis:

```text
G1: script → ver 42
G2: script → ver 43
G2: setLocal(y, 43)
G1: setLocal(x, 42)    // L1 now holds the older value, labelled 42
```

That's what conditional L1 writes are for.

### Conditional L1 writes

- [ ] Add `localCache.setIfNewer`: store only if the incoming version is greater than the version already under the key.
- [ ] Route every L1 write through it: the writer's own write, filling L1 from a Redis hit, and installing a tombstone.

Sketch:

```go
// setIfNewer stores e under key unless the entry already there carries a
// version >= e.version, and reports whether it was stored. Concurrent
// writers can reach the local tier in a different order than they reached
// redis, so the version, not arrival order, decides which one wins.
func (c *localCache) setIfNewer(key string, e *entry) bool {
    n := c.size.Add(1) // speculative, see Set
    for {
        raw, loaded := c.data.LoadOrStore(key, e)
        if !loaded {
            if c.maxSize > 0 && n > int64(c.maxSize) {
                c.evictOne(key)
            }
            return true
        }
        if cur, ok := raw.(*entry); ok && cur.version >= e.version {
            c.size.Add(-1)
            return false // something as new or newer is already here
        }
        if c.data.CompareAndSwap(key, raw, e) {
            c.size.Add(-1) // an update, not an insert
            return true
        }
        // raw was replaced or removed under us: look again
    }
}
```

### Filling L1 from a Redis hit

A fill can land after the invalidation for a newer write:

```text
Node B: L1 miss → GET key            (Redis answers with v41)
Node A: SET → v42, publish(key)
Node B: invalidation arrives → L1.Delete(key)   // nothing to delete yet
Node B: GET reply handled → setLocal(v41)       // stale until localTTL
```

The same race hits `setIfVersion`'s local write when another node writes concurrently.

- [ ] Read value, version and PTTL **atomically** in one small Lua script, replacing the GET+PTTL pipeline in `Get`. The `{key}` hash tag in `versionKey` already puts both keys in the same slot.
- [ ] Install the fill through `setIfNewer`, so a tombstone or newer entry already in L1 wins.
- [ ] A missing version key reads as 0: the fill is installed only if L1 has nothing newer.

### Tombstones

A tombstone keeps the version knowledge around after the value is gone, so a slow fill carrying an older version gets rejected.

- [ ] On receiving `(key, N)`, don't delete the L1 entry: install a tombstone `{value: nil, version: N}` through `setIfNewer`. If L1 already holds version ≥ N, that's a no-op.
- [ ] `Get` treats a tombstone as a miss.
- [ ] Tombstone lifetime: at least as long as a fill can be in flight (the Redis read timeout). `localTTL` is the simple safe default.
- [ ] Decide whether tombstones count toward `maxSize`.

### Self-delivered invalidation

- [ ] Make receiving your own invalidation event harmless.

If:

```text
local version = 42
event version = 42
```

then:

```text
→ no-op
```

This falls out of `setIfNewer`'s `>=`: an event carrying the version already in L1 is a no-op.

- [ ] Once that's in, the `instanceID` skip in `SubscribeInvalidations` is no longer needed for correctness. Keep the node ID for observability only.

---

# 3. TODO — Invalidation transport: Redis Pub/Sub and Redis Streams

Strata supports exactly two invalidation modes, both built on Redis.

Kafka and RPC are **out of scope**:

- Kafka can't be written atomically with the Redis write without an outbox/CDC pipeline, so its durability buys nothing on its own.
- RPC's hard part isn't the transport, it's cluster membership (who are the nodes, what happens when one is down).

### Emit events from inside the Lua write scripts

Both modes emit the `(key, version)` event **inside** `setScript`, `casScript` and `delScript`, atomically with the version bump.

That gives:

- no "write succeeded, publish failed / process crashed" window
- one less round trip per write
- events delivered in version order on a single primary

- [ ] Move `PUBLISH` (or `XADD`) into the write scripts.
- [ ] Remove the separate `tc.redis.Publish` calls from `Set`, `Invalidate` and `setIfVersion`.

### Configuration, not a public interface

The public `InvalidatorV2` interface only existed to let users plug in outside transports. With two built-in modes, a construction option is a smaller promise:

```go
strata.WithInvalidation(strata.PubSub)  // default
strata.WithInvalidation(strata.Streams)
```

- [ ] Replace the public `InvalidatorV2` interface with `WithInvalidation`.
- [ ] Keep an internal interface if useful, but don't commit to a public contract.
- [ ] Since publishing lives in the scripts, what's left to abstract is the **subscribe side**: how a node receives events, and what it does after a gap.

### Redis Pub/Sub (default)

Cheap and low latency.

Properties:

- at-most-once
- non-durable
- messages published while a subscriber is disconnected are lost

- [ ] Flush L1 on resubscribe (go-redis's `ChannelWithSubscriptions` surfaces reconnects), so missed messages cause staleness until the reconnect, not until local TTL.
- [ ] Local TTL remains the last-resort safety net.

### Redis Streams

Durable and replayable, while staying inside Redis.

- [ ] `XADD` the event inside the write scripts, trimmed in the same call with `MAXLEN ~ N`, or the stream grows forever.
- [ ] Each node reads with plain `XREAD`, tracking its own last-seen ID.

    **Not** consumer groups (`XREADGROUP`): every node needs every event, and consumer groups split events across consumers.

- [ ] On reconnect, resume from the last-seen ID.
- [ ] If that ID has already been trimmed away, events were missed: flush L1.
- [ ] Decide the Cluster story up front.

    An `XADD` inside a script must target a stream in the same slot as the key, so a single global stream doesn't work. Options:

    - per-slot streams
    - Streams mode supported only on a single primary / Sentinel

### Important design rule

Do **not** equate transport directly with consistency.

For example:

```text
Streams ≠ automatically fresh caches
Pub/Sub ≠ automatically "bad consistency"
```

The transport provides delivery properties.

Strata should define what those delivery properties mean for cache freshness.

A durable transport only helps if the event is written atomically with the data: a durable log you never got to write to (crash between the Redis write and the publish) is no better than Pub/Sub. This is why both modes emit from inside the Lua scripts.

---

# 4. TODO — Define the consistency model explicitly

- [ ] Document the default consistency model.

Likely model:

> The writer gets immediate local visibility, while other nodes converge asynchronously.

- [ ] Document what happens when an invalidation is missed.

- [ ] Document the role of local TTL.

A missed invalidation can leave a stale L1 entry alive until its local TTL expires.

- [ ] Document that asynchronous invalidation cannot guarantee:

> "After a write completes, no node anywhere can ever return the previous value."

That stronger guarantee requires stronger coordination/read validation.

- [ ] Separate **transport delivery guarantees** from **cache consistency guarantees**.

---

# 5. TODO — Concurrency and correctness

Previously identified issue to revisit:

The local cache has potential ABA-style removal races when code observes an entry and later removes by key.

Potential problematic pattern:

```text
T1:
    observe expired entry

T2:
    Set(key, NEW)

T1:
    LoadAndDelete(key)

→ potentially deletes NEW
```

### Fix / investigate

- [ ] Evaluate identity-based conditional deletion using `sync.Map.CompareAndDelete`.

Conceptually:

```go
current, ok := c.data.Load(key)
if !ok {
    return
}

entry := current.(*entry)

if expired(entry) {
    c.data.CompareAndDelete(key, entry)
}
```

The important distinction is:

> Delete **the entry I observed**, not whatever happens to be under this key now.

### Regression tests

- [ ] Expired `Get` → concurrent `Set` → expiry removal.
- [ ] `sweepExpired` → concurrent `Set` → sweep removal.
- [ ] `evictOne` → concurrent `Set` → eviction removal.
- [ ] Verify size accounting under concurrent `Set/Delete/Evict/Expire`.
- [ ] Run race detector/stress tests against the new version-aware invalidation state machine.

---

# 6. TODO — Version lifecycle

The Redis version key introduces another design question.

For example:

```text
strata:ver:{key}
```

If the actual cache value disappears but the version key remains, high-cardinality transient keys could leave behind version metadata indefinitely.

However, deleting the version immediately can introduce stale-loader resurrection problems.

### Current state: version keys leak

`setScript`, `casScript` and `delScript` never set an expiry on the version key. Every key ever written leaves `strata:ver:{key}` behind forever.

### Why a naive TTL is unsafe

If the version key expires, the counter restarts and the CAS can be fooled:

```text
Loader:      reads version → absent (0), starts loading
Invalidate:  DEL value, INCR version → 1
             version key expires → absent (0) again
Loader:      CAS expects 0, sees 0 → writes the value it loaded before the Invalidate
```

A restart also breaks the nodes: one still holding an L1 entry at version 7 ignores a new write's event at version 1, so it serves its stale value until local TTL.

### The rule

A version key must outlive anything that could still act on the version it holds:

- a loader's CAS. That's bounded by `loaderTimeout`, because `GetOrLoad` cancels the loader's context, so the CAS can't be sent after it.
- L1 entries and tombstones carrying that version. Those are bounded by their local TTL, which is capped by the write's remote TTL.

- [ ] Set the version key's expiry in the same script that bumps it.
- [ ] `setScript` / `casScript`: version TTL ≥ that write's remote TTL + `loaderTimeout` + margin.
- [ ] `delScript`: version TTL ≥ tombstone lifetime + `loaderTimeout` + margin.
- [ ] Never shorten the version key's TTL. A later short-TTL write must not cut short an L1 entry from an earlier, longer-TTL write.

    Gotcha: `PEXPIRE ... GT` treats a key with no expiry as infinite, so it won't set the first TTL on a freshly `INCR`'d version key. Handle that case explicitly.

- [ ] A write with no remote expiry → the version key has no expiry either.
- [ ] If `loaderTimeout` is disabled (≤ 0), nothing bounds an in-flight CAS: version keys must never expire in that mode, or the timeout becomes mandatory.
- [ ] Investigate minting versions as `max(current + 1, Redis TIME in µs)`, so a recreated version key never restarts below versions nodes still remember. Depends on Redis's clock not stepping backwards (failover, NTP).

### Invariants

- [ ] Document the versioning invariants.

> A stale loader must never overwrite a newer version.

> A node's knowledge of a key's version never moves backwards, so Redis must never hand out a version below one a node may still remember.

---

# 7. TODO — Design questions

These are the rabbit holes worth keeping track of.

- [ ] Should an invalidation immediately delete the L1 entry?
- [ ] Or should the entry be retained but marked stale?
- [ ] Should an L1 entry contain `value + version`, or also `highestKnownVersion` / invalidation state?
- [ ] How should out-of-order events be handled?
- [ ] Should invalidation events contain an origin/node ID?
- [ ] Should they contain an event ID for observability/deduplication?
- [ ] Should the event type remain `Invalidation`, or should it be called something more accurate such as `VersionUpdate`?
- [ ] Can the same event model cleanly support both Pub/Sub and Streams?
- [ ] What exact guarantees should each invalidation mode advertise?
- [ ] Should the interface be called `Invalidator`, `VersionPublisher`, or another name?
- [ ] Can the system provide stronger guarantees without putting Redis on the normal L1 read path?

---

# 8. The consistency problem in one picture

The key distinction:

```text
                    Redis
               authoritative-ish
                    state
                      │
          ┌───────────┴───────────┐
          │                       │
       version/CAS            version event
          │                       │
          ↓                       ↓
   stale writers              stale L1s
          │                       │
          ↓                       ↓
 write correctness          read freshness
```

### Version/CAS solves:

> "Can an old/slow loader still overwrite a newer value?"

### Version-aware invalidation solves:

> "How do other nodes learn that their local copy is stale without querying Redis on every read?"

These are two different distributed-systems problems.

---

# 9. Project description — what Strata actually is

## Full description

**Strata is a high-performance two-tier Go cache designed around concurrent loading, distributed invalidation, and version-based stale-write prevention.**

It combines an in-process L1 cache with Redis as a shared L2, while using versioned state to prevent slow or stale loaders from overwriting newer data.

The system is designed for multi-node applications where local memory provides the fast hot path, while Redis provides shared state and coordination.

Cache freshness is propagated between nodes through an **asynchronous invalidation mechanism**, rather than forcing every L1 read to perform a network version check.

Invalidation events travel through Redis itself: lightweight Redis Pub/Sub by default, or durable, replayable Redis Streams. In both cases the event is emitted atomically with the write that produced it.

Strata does not attempt to provide one universal consistency model. Instead, it provides mechanisms that let applications choose an appropriate coherence strategy while making the tradeoffs between latency, durability, delivery guarantees, availability, and stale reads explicit.

---

# 10. Short GitHub description

> **A high-performance two-tier Go cache with Redis-backed shared state, version-based stale-write prevention, request coalescing, and distributed cache coherence over Redis Pub/Sub or Streams.**

---

# 11. Mental model to remember

Strata is **not merely "local cache + Redis."**

Think of it as:

```text
                    ┌──────────────────────┐
                    │        Strata        │
                    │                      │
                    │  L1       L2         │
                    │ local    Redis       │
                    │                      │
                    │ versioning           │
                    │ singleflight         │
                    │ stale-write CAS      │
                    │ coherence            │
                    └──────────┬───────────┘
                               │
                         invalidation
                     (emitted from Lua)
                               │
                      ┌────────┴────────┐
                      ↓                 ↓
                   Pub/Sub           Streams
```

The key idea is:

> **The L1 cache is the fast data path. Redis provides shared state and versioning. Version-bearing events propagate knowledge of freshness between replicas. The invalidation transport determines how quickly and reliably replicas learn that their local state is stale.**

---

# 12. Batching support for the commands

- [ ] Add batch loader as well
- [ ] Add batching support for all the commands

---

# 13. Where the project came from

Strata is part of a broader systems-learning trajectory:

```text
Redis protocol implementation
          ↓
understand the primitive
          ↓
Strata
          ↓
understand caching/concurrency
          ↓
distributed coherence
          ↓
consistency + delivery semantics
```

The interesting direction is not just implementing infrastructure.

It is understanding **what guarantees the underlying primitives provide, what they do not provide, and how to compose them into a useful higher-level system.**
