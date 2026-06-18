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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ============================================================================
// Rollout completion detection
// ============================================================================

// deploymentIsComplete returns true when all five conditions for a completed
// rollout are satisfied simultaneously.
//
// This matches what kubectl rollout status uses internally. All five must be
// true — any single condition alone is insufficient.
func deploymentIsComplete(d *appsv1.Deployment) bool {
	desired := desiredReplicas(d)

	// 1. Controller has processed the current spec version.
	if d.Status.ObservedGeneration < d.Generation {
		return false
	}

	// 2. All pods have been updated to the new version.
	if d.Status.UpdatedReplicas != desired {
		return false
	}

	// 3. All pods are available (passing readiness probes).
	if d.Status.AvailableReplicas != desired {
		return false
	}

	// 4. No pods are in a failing or terminating state.
	if d.Status.UnavailableReplicas != 0 {
		return false
	}

	// 5. The Available condition is explicitly True.
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}

	return false
}

// desiredReplicas returns the desired replica count for a Deployment.
// When spec.replicas is nil, Kubernetes defaults to 1.
func desiredReplicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}

// ============================================================================
// Primary container resolution
// ============================================================================

// resolvePrimaryContainer returns the index of the primary container in the
// given pod spec, applying the three-step resolution order:
//
//  1. If primaryContainerName is set, find that container by name.
//  2. If exactly one non-init container exists, use it.
//  3. Fall back to index 0.
//
// Returns the index and a boolean indicating whether the fallback to index 0
// was used (so the caller can emit a PrimaryContainerInferred condition).
func resolvePrimaryContainer(spec corev1.PodSpec, primaryContainerName string) (index int, inferred bool) {
	// Step 1: explicit name configured on the policy.
	if primaryContainerName != "" {
		for i, c := range spec.Containers {
			if c.Name == primaryContainerName {
				return i, false
			}
		}
		// Named container not found — fall through to step 2.
		// The controller will emit a warning condition separately.
	}

	// Step 2: exactly one non-init container.
	if len(spec.Containers) == 1 {
		return 0, false
	}

	// Step 3: fall back to index 0 with inferred=true.
	return 0, true
}

// ============================================================================
// Image digest extraction
// ============================================================================

// imageDigestForDeployment finds a running Pod owned by the Deployment's
// current ReplicaSet and returns the image digest of the primary container.
//
// This is the runtime truth — the actual digest that Kubernetes pulled and ran,
// not the tag in the Deployment spec.
func imageDigestForDeployment(
	ctx context.Context,
	c client.Client,
	d *appsv1.Deployment,
	primaryContainerName string,
) (digest string, inferred bool, err error) {
	// ── 1. Find the current ReplicaSet ───────────────────────────────────
	//
	// A Deployment manages multiple ReplicaSets — one per rollout revision.
	// The current one is identified by matching the pod-template-hash label
	// that Kubernetes stamps on every ReplicaSet and its Pods.

	rsList := &appsv1.ReplicaSetList{}
	if err := c.List(ctx, rsList,
		client.InNamespace(d.Namespace),
		client.MatchingLabels(d.Spec.Selector.MatchLabels),
	); err != nil {
		return "", false, fmt.Errorf("listing replicasets: %w", err)
	}

	currentRS := currentReplicaSet(d, rsList.Items)
	if currentRS == nil {
		return "", false, fmt.Errorf("no current replicaset found for deployment %s/%s",
			d.Namespace, d.Name)
	}

	// ── 2. Find a running Pod owned by the current ReplicaSet ────────────

	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(d.Namespace),
		client.MatchingLabels(currentRS.Spec.Selector.MatchLabels),
	); err != nil {
		return "", false, fmt.Errorf("listing pods: %w", err)
	}

	pod := runningPod(podList.Items)
	if pod == nil {
		return "", false, fmt.Errorf("no running pod found for replicaset %s/%s",
			currentRS.Namespace, currentRS.Name)
	}

	// ── 3. Resolve the primary container ─────────────────────────────────

	containerIndex, inferred := resolvePrimaryContainer(pod.Spec, primaryContainerName)

	// ── 4. Read the image digest from containerStatuses ──────────────────
	//
	// pod.status.containerStatuses[i].imageID is the runtime truth.
	// It is always a fully resolved digest: "docker.io/library/nginx@sha256:abc123"
	// We strip the registry prefix to get the bare digest.

	if containerIndex >= len(pod.Status.ContainerStatuses) {
		return "", inferred, fmt.Errorf(
			"container index %d out of range in pod %s/%s (have %d statuses)",
			containerIndex, pod.Namespace, pod.Name, len(pod.Status.ContainerStatuses),
		)
	}

	imageID := pod.Status.ContainerStatuses[containerIndex].ImageID
	digest = extractDigest(imageID)
	if digest == "" {
		return "", inferred, fmt.Errorf("could not extract digest from imageID %q", imageID)
	}

	return digest, inferred, nil
}

// currentReplicaSet returns the ReplicaSet that owns the Deployment's current
// pod template. It matches by pod-template-hash annotation which Kubernetes
// stamps on every ReplicaSet it creates.
func currentReplicaSet(d *appsv1.Deployment, rsList []appsv1.ReplicaSet) *appsv1.ReplicaSet {
	// The current RS is the one whose pod template hash matches the
	// hash in the Deployment's pod template labels.
	templateHash := d.Spec.Template.Labels["pod-template-hash"]

	for i := range rsList {
		rs := &rsList[i]

		// Match by pod-template-hash label.
		if rs.Labels["pod-template-hash"] == templateHash {
			return rs
		}

		// Also match by owner reference — more reliable than label matching
		// in edge cases where labels are customised.
		for _, ref := range rs.OwnerReferences {
			if ref.UID == d.UID && ref.Controller != nil && *ref.Controller {
				// Among all owned RSes, pick the one with the most replicas
				// matching the current template. For now take the first match.
				return rs
			}
		}
	}

	return nil
}

// runningPod returns the first Pod in Running phase with all containers ready.
func runningPod(pods []corev1.Pod) *corev1.Pod {
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if !allContainersReady(pod) {
			continue
		}
		return pod
	}
	return nil
}

// allContainersReady returns true when every container in the Pod is ready.
func allContainersReady(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if !cs.Ready {
			return false
		}
	}
	return len(pod.Status.ContainerStatuses) > 0
}

// extractDigest extracts the sha256 digest from an imageID string.
//
// imageID formats observed in the wild:
//
//	docker.io/library/nginx@sha256:abc123...
//	sha256:abc123...
//	registry.example.com/myapp@sha256:abc123...
func extractDigest(imageID string) string {
	// Find the sha256: prefix and return everything from there.
	idx := strings.Index(imageID, "sha256:")
	if idx == -1 {
		return ""
	}
	return imageID[idx:]
}

// ============================================================================
// Label selector helpers
// ============================================================================

// labelsMatch returns true when the given labels satisfy the selector.
// Used to match Pods to ReplicaSets without a List call.
func labelsMatch(selector *appsv1.DeploymentSpec, podLabels map[string]string) bool {
	sel, err := labels.Parse(selector.Selector.String())
	if err != nil {
		return false
	}
	return sel.Matches(labels.Set(podLabels))
}
