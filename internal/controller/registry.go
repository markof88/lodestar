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
	"context"
	"fmt"
	"time"

	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// ============================================================================
// OCI label keys
// ============================================================================

const (
	// labelCreated is the standard OCI label for image build time.
	// https://github.com/opencontainers/image-spec/blob/main/annotations.md
	labelCreated = "org.opencontainers.image.created"

	// labelRevision is the standard OCI label for the source control revision
	// (typically a git commit SHA) the image was built from.
	labelRevision = "org.opencontainers.image.revision"
)

// ============================================================================
// Registry client
// ============================================================================

// registryClient reads OCI image metadata from container registries,
// authenticating with the same credentials the kubelet used to pull the
// image — imagePullSecrets on the Pod's ServiceAccount, or cloud workload
// identity (IRSA, Workload Identity, ACR) when no explicit secret exists.
type registryClient struct {
	// clientset is the classic Kubernetes client, required by k8schain to
	// resolve imagePullSecrets and ServiceAccount credentials.
	clientset kubernetes.Interface

	// cache stores fetched metadata permanently, keyed by digest.
	cache *digestCache
}

// newRegistryClient constructs a registryClient.
func newRegistryClient(clientset kubernetes.Interface, cache *digestCache) *registryClient {
	return &registryClient{
		clientset: clientset,
		cache:     cache,
	}
}

// imageRef holds the information needed to look up an image's metadata.
type imageRef struct {
	// Repository is the image reference without digest, e.g.
	// "ghcr.io/markof88/myapp". Used to resolve the registry hostname for auth.
	Repository string

	// Digest is the content digest, e.g. "sha256:abc123...".
	Digest string

	// Namespace is the Pod's namespace, used to resolve imagePullSecrets.
	Namespace string

	// ServiceAccountName is the Pod's service account, used to resolve
	// imagePullSecrets attached to the ServiceAccount.
	ServiceAccountName string

	// ImagePullSecrets are the names of secrets referenced directly on the Pod.
	ImagePullSecrets []string
}

// GetMetadata returns the OCI labels for the given image, using the cache
// when available and falling back to a registry call otherwise.
//
// Because digests are immutable, a successful fetch is cached forever — the
// next call for the same digest never touches the network again.
func (r *registryClient) GetMetadata(ctx context.Context, ref imageRef) (imageMetadata, error) {
	if cached, ok := r.cache.get(ref.Digest); ok {
		return cached, nil
	}

	meta, err := r.fetchMetadata(ctx, ref)
	if err != nil {
		return imageMetadata{}, err
	}

	r.cache.set(ref.Digest, meta)
	return meta, nil
}

// fetchMetadata performs the actual registry call.
func (r *registryClient) fetchMetadata(ctx context.Context, ref imageRef) (imageMetadata, error) {
	// ── 1. Parse the image reference ─────────────────────────────────────
	//
	// We construct a digest reference ("repo@sha256:...") rather than a tag
	// reference, since the digest is the runtime truth we already extracted
	// from the Pod's containerStatuses.

	fullRef := fmt.Sprintf("%s@%s", ref.Repository, ref.Digest)
	parsedRef, err := name.NewDigest(fullRef)
	if err != nil {
		return imageMetadata{}, fmt.Errorf("parsing image reference %q: %w", fullRef, err)
	}

	// ── 2. Build the keychain from the Pod's pull credentials ───────────
	//
	// k8schain understands:
	//   - imagePullSecrets on the Pod spec
	//   - imagePullSecrets on the Pod's ServiceAccount
	//   - cloud workload identity (IRSA, GKE Workload Identity, ACR) as a
	//     fallback when no explicit secret matches the registry
	//
	// This means Lodestar never needs its own registry credentials.

	keychain, err := k8schain.New(ctx, r.clientset, k8schain.Options{
		Namespace:          ref.Namespace,
		ServiceAccountName: ref.ServiceAccountName,
		ImagePullSecrets:   ref.ImagePullSecrets,
	})
	if err != nil {
		return imageMetadata{}, fmt.Errorf("building keychain: %w", err)
	}

	// ── 3. Fetch the image config ────────────────────────────────────────
	//
	// remote.Image fetches the manifest and config blob. The config blob
	// contains the OCI labels we need — it does NOT download any layers,
	// so this is a lightweight call regardless of image size.

	img, err := remote.Image(parsedRef, remote.WithAuthFromKeychain(keychain), remote.WithContext(ctx))
	if err != nil {
		return imageMetadata{}, fmt.Errorf("fetching image %s: %w", fullRef, err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		return imageMetadata{}, fmt.Errorf("reading image config for %s: %w", fullRef, err)
	}

	// ── 4. Extract OCI labels ────────────────────────────────────────────

	meta := imageMetadata{}

	if createdStr, ok := configFile.Config.Labels[labelCreated]; ok {
		if parsed, err := time.Parse(time.RFC3339, createdStr); err == nil {
			meta.CreatedAt = &parsed
		}
		// A malformed timestamp is not a fatal error — we simply omit
		// Lead Time for this deployment rather than guessing.
	}

	if revision, ok := configFile.Config.Labels[labelRevision]; ok {
		meta.Revision = revision
	}

	return meta, nil
}

// podPullSecretNames extracts the names of imagePullSecrets referenced
// directly on a Pod spec.
func podPullSecretNames(spec corev1.PodSpec) []string {
	names := make([]string, 0, len(spec.ImagePullSecrets))
	for _, ref := range spec.ImagePullSecrets {
		names = append(names, ref.Name)
	}
	return names
}
