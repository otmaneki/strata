package strata

import (
	"context"
	"fmt"
)

// GetOrLoad implements Loader for TieredCache.
// On a miss, it reads key's version before running loader, so
// setIfVersion can later tell whether a concurrent write landed while the
// (possibly slow) loader was still running and if so drop the loader's
// now-stale result instead of caching it.
func (tc *TieredCache) GetOrLoad(ctx context.Context, key string, loader func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	if val, ok := tc.Get(ctx, key); ok {
		return val, nil
	}

	result, err, _ := tc.sf.Do(key, func() (result any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("loader for key %s panicked: %v", key, r)
			}
		}()

		detachedCtx := context.WithoutCancel(ctx)
		if tc.loaderTimeout > 0 {
			var cancel context.CancelFunc
			detachedCtx, cancel = context.WithTimeout(detachedCtx, tc.loaderTimeout)
			defer cancel()
		}

		// Double-check: another goroutine may have filled the cache
		// while we were waiting for a slot in singleflight.
		if val, ok := tc.Get(detachedCtx, key); ok {
			return val, nil
		}

		// Captured before, not after, loader runs: see setIfVersion.
		var expectedVersion int64
		var versionErr error
		if tc.remoteEnabled {
			expectedVersion, versionErr = tc.currentVersion(detachedCtx, key)
		}

		val, err := loader(detachedCtx)
		if err != nil {
			return nil, fmt.Errorf("loader for key %s: %w", key, err)
		}

		// Fail open: the loader already did the real work, so a
		// cache-population failure is reported via Observer, not
		// returned, and shouldn't fail this call too.
		switch {
		case !tc.remoteEnabled:
			// No concurrent redis writer to race against, so Set's plain
			// last-write-wins is fine.
			if err := tc.Set(detachedCtx, key, val); err != nil {
				tc.stats.setErrors.Add(1)
				tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
			}
		case versionErr != nil:
			tc.stats.setErrors.Add(1)
			tc.observer.OnSetError(fmt.Errorf("read version for key %s: %w", key, versionErr))
		default:
			switch applied, err := tc.setIfVersion(detachedCtx, key, val, expectedVersion); {
			case err != nil:
				tc.stats.setErrors.Add(1)
				tc.observer.OnSetError(fmt.Errorf("set key %s: %w", key, err))
			case !applied:
				tc.stats.staleWritesDropped.Add(1)
			}
		}

		return val, nil
	})
	if err != nil {
		return nil, err
	}

	// result is always []byte: it's either what Get returned or what loader
	// returned, both of which are []byte by construction. Guard it anyway
	// rather than risk a panic.
	b, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T for key %s", result, key)
	}
	return b, nil
}
