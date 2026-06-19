/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"sync"
	"time"
)

// ============================================================================
// Image metadata cache
// ============================================================================

// imageMetadata holds the OCI labels we care about for a given image digest.
type imageMetadata struct {
	// CreatedAt is parsed from the org.opencontainers.image.created label.
	// Nil when the label was absent or unparseable.
	CreatedAt *time.Time

	// Revision is the org.opencontainers.image.revision label value
	// (typically a git commit SHA). Empty when absent.
	Revision string
}

// digestCache caches image digest to metadata in memory.
//
// Digests are immutable by definition — sha256:abc123 always points to the
// exact same image content. This means cached entries never need to expire
// or be invalidated due to the underlying image changing.
//
// The cache is intentionally unbounded. In practice the number of distinct
// digests observed by a single Lodestar instance over its lifetime is small
// (one entry per deployed image version), so memory growth is not a concern
// at realistic scale.
type digestCache struct {
	entries sync.Map // map[string]imageMetadata
}

// newDigestCache constructs an empty cache.
func newDigestCache() *digestCache {
	return &digestCache{}
}

// get returns the cached metadata for a digest, and whether it was found.
func (c *digestCache) get(digest string) (imageMetadata, bool) {
	v, ok := c.entries.Load(digest)
	if !ok {
		return imageMetadata{}, false
	}
	return v.(imageMetadata), true
}

// set stores metadata for a digest. Safe to call concurrently; if multiple
// goroutines race to set the same digest, the last write wins — harmless
// since all writers would compute the same value for the same digest.
func (c *digestCache) set(digest string, meta imageMetadata) {
	c.entries.Store(digest, meta)
}

// invalidate removes a cached entry, forcing the next lookup to re-fetch
// from the registry.
func (c *digestCache) invalidate(digest string) {
	c.entries.Delete(digest)
}

// invalidateAll clears every cached entry. Used by the
// lodestar.io/refresh-image-cache annotation for operational debugging —
// when someone suspects stale OCI label data, this forces a full re-fetch
// on the next observation cycle.
func (c *digestCache) invalidateAll() {
	c.entries.Range(func(key, _ any) bool {
		c.entries.Delete(key)
		return true
	})
}
