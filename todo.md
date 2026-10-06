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
pluggable invalidation transport
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

- [ ] After a successful write/version advance, the node that performed the write should immediately update its own L1.

Desired flow:

```text
Node A receives SET
       ↓
Redis value + version updated
       ↓
Node A L1 updated immediately
       ↓
publish (key, version)
       ↓
other nodes converge
```

The writer should **not** need to wait for invalidation propagation before serving the new value locally.

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

---

# 3. TODO — Abstract the invalidation transport

Do not tightly couple Strata's coherence mechanism to Redis Pub/Sub.

The cache should depend on a small abstraction.

Suggested interface:

```go
type Invalidator interface {
    Invalidate(ctx context.Context, event Invalidation) error
}
```

Then:

```text
                     Strata
                       │
                 Invalidator
                       │
        ┌──────────────┼──────────────┐
        ↓              ↓              ↓
    Redis Pub/Sub   Redis Streams    Kafka
                       │
                       │
                      RPC
```

### Possible implementations

- [ ] Redis Pub/Sub

    Cheap and low latency.

    Typical property:

    - at-most-once
    - non-durable
    - missed messages are possible

    Local TTL remains an important safety net.

- [ ] Redis Streams

    Useful when durable/replayable invalidation is desired while Redis is already part of the architecture.

- [ ] Kafka

    Useful for users who already operate Kafka and want durable/replayable event propagation.

- [ ] RPC

    Useful when a deployment wants synchronous/acknowledged propagation and tighter freshness semantics.

### Important design rule

Do **not** equate transport directly with consistency.

For example:

```text
RPC ≠ automatically strong consistency
Kafka ≠ automatically eventual consistency
Pub/Sub ≠ automatically "bad consistency"
```

The transport provides delivery properties.

Strata should define what those delivery properties mean for cache freshness.

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

Therefore:

- [ ] Decide how long version keys should live.
- [ ] Investigate version-key TTLs.
- [ ] Investigate cleanup strategies.
- [ ] Ensure cleanup cannot allow an old loader to overwrite a newer logical generation.
- [ ] Document the versioning invariant.

Core invariant:

> A stale loader must never overwrite a newer version.

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
- [ ] Can the same event model cleanly support Pub/Sub, Streams, Kafka and RPC?
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

Cache freshness is propagated between nodes through a **pluggable invalidation mechanism**, rather than forcing every L1 read to perform a network version check.

The invalidation layer is intentionally transport-agnostic. Deployments can choose lightweight Redis Pub/Sub, durable/replayable Redis Streams or Kafka, or synchronous RPC-style propagation depending on their operational requirements and desired delivery semantics.

Strata does not attempt to provide one universal consistency model. Instead, it provides mechanisms that let applications choose an appropriate coherence strategy while making the tradeoffs between latency, durability, delivery guarantees, availability, and stale reads explicit.

---

# 10. Short GitHub description

> **A high-performance two-tier Go cache with Redis-backed shared state, version-based stale-write prevention, request coalescing, and pluggable distributed cache coherence.**

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
                         Invalidator
                               │
              ┌────────────────┼────────────────┐
              ↓                ↓                ↓
          Pub/Sub           Streams           Kafka/RPC
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
