package strata

import (
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	value     any
	expiresAt time.Time
}

// localCache is an in-process, TTL-based cache with an approximate size
// bound. It's built on sync.Map for lock-free reads, which is why the bound
// is "best effort" rather than exact: sync.Map doesn't track insertion or
// access order, so eviction picks an arbitrary victim rather than the
// least-recently-used one, and the grow-then-evict sequence in Set isn't
// atomic under concurrent writers. For a cache (not an authoritative store)
// that trade-off is the right one, it keeps reads free of locking.
type localCache struct {
	data sync.Map
	// maxSize <= 0 means unbounded: no eviction on write
	// basically a fancy memory leak :D
	maxSize  int
	size     atomic.Int64
	stop     chan struct{}
	stopOnce sync.Once
}

// newLocalCache starts a localCache along with its background TTL sweeper.
// Callers must call Close when done with the cache, or the sweeper
// goroutine leaks for the life of the process.
func newLocalCache(maxSize int, evictInterval time.Duration) *localCache {
	c := &localCache{maxSize: maxSize, stop: make(chan struct{})}
	go c.evictLoop(evictInterval)
	return c
}

// Close stops the background TTL sweeper. Safe to call more than once.
func (c *localCache) Close() {
	c.stopOnce.Do(func() { close(c.stop) })
}

// Get does exactly what it says.
func (c *localCache) Get(key string) (any, bool) {
	raw, ok := c.data.Load(key)
	if !ok {
		return nil, false
	}
	e, ok := raw.(*entry)
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expiresAt) {
		// LoadAndDelete, not Delete: two concurrent Gets can both observe
		// the same expired entry before either removes it. Delete alone
		// can't tell you whether *this* call was the one that actually
		// removed something, so decrementing unconditionally after it
		// double-counts the eviction and drifts size negative.
		if _, loaded := c.data.LoadAndDelete(key); loaded {
			c.size.Add(-1)
		}
		return nil, false
	}

	return e.value, true
}

// Set stores value under key with the given ttl. If the cache is at
// capacity and key is new, Set evicts one arbitrary existing entry to make
// room; updating an already-present key never triggers eviction, since that
// doesn't grow the cache.
func (c *localCache) Set(key string, value any, ttl time.Duration) {
	e := &entry{value: value, expiresAt: time.Now().Add(ttl)}

	// size is incremented speculatively, before Swap makes the entry
	// visible to other goroutines. Doing it the other way around, Swap,
	// then increment, leaves a window where a concurrent Delete can
	// observe the freshly-stored entry and decrement size before this call
	// gets to its own increment, which is enough for Len() to be observed
	// transiently negative. If this Set turns out to be an update rather
	// than an insert, the speculative increment is undone below.
	n := c.size.Add(1)

	if _, loaded := c.data.Swap(key, e); loaded {
		c.size.Add(-1)
		return
	}

	if c.maxSize > 0 && n > int64(c.maxSize) {
		c.evictOne(key)
	}
}

// Delete does also exactly what it says.
func (c *localCache) Delete(key string) {
	if _, loaded := c.data.LoadAndDelete(key); loaded {
		c.size.Add(-1)
	}
}

// Len returns the approximate number of entries currently held. It's
// approximate because size is tracked separately from the underlying map to
// keep reads lock-free, so it can be briefly stale relative to concurrent
// writers.
func (c *localCache) Len() int {
	return int(c.size.Load())
}

func (c *localCache) evictLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.sweepExpired()
		}
	}
}

// evictOne removes one entry other than except (the key that was just
// inserted) to bring the cache back under its size bound. The victim is
// whatever sync.Map's Range happens to visit first. There's no
// access-order tracking to do better than that without giving up the
// lock-free read path.
func (c *localCache) evictOne(except string) {
	c.data.Range(func(key, _ any) bool {
		if key == except {
			return true // keep scanning
		}
		// LoadAndDelete, see the comment in Get. Another goroutine (a
		// concurrent evictOne, or an expiry in Get/sweepExpired) may have
		// already removed this exact key between Range visiting it and us
		// calling Delete.
		if _, loaded := c.data.LoadAndDelete(key); loaded {
			c.size.Add(-1)
		}
		return false
	})
}

func (c *localCache) sweepExpired() {
	now := time.Now()
	c.data.Range(func(key, value any) bool {
		e, ok := value.(*entry)
		if !ok {
			return true // skip this one, keep scanning the rest
		}
		if now.After(e.expiresAt) {
			// LoadAndDelete, see the comment in Get. A concurrent Get on
			// this same key may be expiring and removing it at the same
			// time.
			if _, loaded := c.data.LoadAndDelete(key); loaded {
				c.size.Add(-1)
			}
		}
		return true
	})
}
