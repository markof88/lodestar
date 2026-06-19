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
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodestarv1alpha1 "github.com/markof88/lodestar/api/v1alpha1"
)

// ============================================================================
// Condition type constants
// ============================================================================

const (
	ConditionReady                    = "Ready"
	ConditionConflict                 = "Conflict"
	ConditionPrimaryContainerInferred = "PrimaryContainerInferred"
)

// registryClient returns the shared registryClient instance, constructing
// it on first use. Lazy construction avoids requiring Clientset to be set
// in tests that don't exercise Lead Time calculation.
func (r *DORAPolicyReconciler) getRegistryClient() *registryClient {
	r.registryOnce.Do(func() {
		r.registry = newRegistryClient(r.Clientset, newDigestCache())
	})
	return r.registry
}

// ============================================================================
// Reconciler
// ============================================================================

// DORAPolicyReconciler reconciles DORAPolicy objects.
type DORAPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Clientset is the classic Kubernetes client, used by k8schain to
	// resolve imagePullSecrets and ServiceAccount credentials when reading
	// OCI image metadata from container registries.
	Clientset kubernetes.Interface

	// registry lazily constructed on first use — see registryClient().
	registry     *registryClient
	registryOnce sync.Once
}

// +kubebuilder:rbac:groups=lodestar.io,resources=dorapolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=lodestar.io,resources=dorapolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=lodestar.io,resources=dorapolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

// Reconcile is called by controller-runtime whenever a DORAPolicy or a watched secondary resource (Deployment, Namespace) changes.
//
// It must be idempotent, calling it twice with the same state must produce the same result.
func (r *DORAPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// ── 1. Fetch the DORAPolicy ───────────────────────────────────────────

	policy := &lodestarv1alpha1.DORAPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted between event being queued and us processing it.
			// Nothing to do, owned resources are garbage-collected by Kubernetes.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching DORAPolicy: %w", err)
	}

	log.Info("reconciling", "environment", policy.Spec.Environment)

	// ── 1b. Handle cache refresh annotation ────────────────────────────────

	if err := r.handleCacheRefreshAnnotation(ctx, policy); err != nil {
		log.Error(err, "handling cache refresh annotation")
	}

	// ── 2. Resolve selected namespaces ───────────────────────────────────

	namespaces, err := r.resolveNamespaces(ctx, policy)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving namespaces: %w", err)
	}

	// ── 3. Detect namespace conflicts ────────────────────────────────────

	conflictReason, err := r.detectConflict(ctx, policy, namespaces)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("detecting conflicts: %w", err)
	}

	if conflictReason != "" {
		setCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               ConditionConflict,
			Status:             metav1.ConditionTrue,
			Reason:             "NamespaceAlreadySelected",
			Message:            conflictReason,
			ObservedGeneration: policy.Generation,
		})
		if err := r.Status().Update(ctx, policy); err != nil {
			log.Error(err, "updating conflict condition failed")
			return ctrl.Result{}, fmt.Errorf("updating conflict condition: %w", err)
		}
		return ctrl.Result{}, nil
	}

	removeCondition(&policy.Status.Conditions, ConditionConflict)

	// ── 4. Update observed namespaces ────────────────────────────────────

	policy.Status.ObservedNamespaces = namespaces

	// ── 5. Observe deployments in selected namespaces ─────────────────────

	if err := r.observeDeployments(ctx, policy, namespaces); err != nil {
		log.Error(err, "observing deployments")
	}

	// ── 6. Garbage-collect stale workload entries ─────────────────────────

	if err := r.gcWorkloads(ctx, policy, namespaces); err != nil {
		// Non-fatal, log and continue. Will succeed on next reconcile.
		log.Error(err, "garbage collecting workload status")
	}

	// ── 7. Mark Ready ────────────────────────────────────────────────────

	setCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            fmt.Sprintf("observing %d namespace(s)", len(namespaces)),
		ObservedGeneration: policy.Generation,
	})

	// ── 8. Persist status ────────────────────────────────────────────────

	if err := r.Status().Update(ctx, policy); err != nil {
		log.Error(err, "updating status failed")
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	log.Info("reconciled", "namespaces", namespaces)
	return ctrl.Result{}, nil
}

const annotationRefreshImageCache = "lodestar.io/refresh-image-cache"

// handleCacheRefreshAnnotation clears the image metadata cache when the
// lodestar.io/refresh-image-cache annotation is present, then removes the
// annotation so it does not trigger on every subsequent reconcile.
func (r *DORAPolicyReconciler) handleCacheRefreshAnnotation(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
) error {
	if policy.Annotations[annotationRefreshImageCache] != "true" {
		return nil
	}

	log := logf.FromContext(ctx)
	log.Info("refreshing image metadata cache", "policy", policy.Name)

	r.getRegistryClient().cache.invalidateAll()

	delete(policy.Annotations, annotationRefreshImageCache)
	if err := r.Update(ctx, policy); err != nil {
		return fmt.Errorf("removing refresh annotation: %w", err)
	}

	return nil
}

// ============================================================================
// Namespace resolution
// ============================================================================

// resolveNamespaces returns the names of namespaces this policy observes.
// When NamespaceSelector is nil, returns only the policy's own namespace.
func (r *DORAPolicyReconciler) resolveNamespaces(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
) ([]string, error) {
	if policy.Spec.NamespaceSelector == nil {
		return []string{policy.Namespace}, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(policy.Spec.NamespaceSelector)
	if err != nil {
		return nil, fmt.Errorf("parsing namespace selector: %w", err)
	}

	nsList := &corev1.NamespaceList{}
	if err := r.List(ctx, nsList, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}

	names := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		names = append(names, ns.Name)
	}
	return names, nil
}

// ============================================================================
// Conflict detection
// ============================================================================

// detectConflict returns a non-empty reason string if any namespace in the given list is already claimed by an older DORAPolicy.
func (r *DORAPolicyReconciler) detectConflict(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
	namespaces []string,
) (string, error) {
	allPolicies := &lodestarv1alpha1.DORAPolicyList{}
	if err := r.List(ctx, allPolicies); err != nil {
		return "", fmt.Errorf("listing policies: %w", err)
	}

	ours := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		ours[ns] = struct{}{}
	}

	for i := range allPolicies.Items {
		other := &allPolicies.Items[i]

		// Skip ourselves.
		if other.UID == policy.UID {
			continue
		}

		// Determine priority: older timestamp wins.
		// When timestamps are equal (same second), smaller UID wins —
		// UIDs are UUIDs assigned at creation time so this is stable.
		otherIsOlder := other.CreationTimestamp.Before(&policy.CreationTimestamp) ||
			(other.CreationTimestamp.Equal(&policy.CreationTimestamp) &&
				string(other.UID) < string(policy.UID))

		if !otherIsOlder {
			continue
		}

		otherNS, err := r.resolveNamespaces(ctx, other)
		if err != nil {
			continue
		}

		for _, ns := range otherNS {
			if _, overlap := ours[ns]; overlap {
				return fmt.Sprintf(
					"namespace %q already selected by DORAPolicy %s/%s",
					ns, other.Namespace, other.Name,
				), nil
			}
		}
	}

	return "", nil
}

// ============================================================================
// Garbage collection
// ============================================================================

// gcWorkloads removes status.workloads entries for Deployments that no longer exist in the observed namespaces.
func (r *DORAPolicyReconciler) gcWorkloads(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
	namespaces []string,
) error {
	if len(policy.Status.Workloads) == 0 {
		return nil
	}

	existing := make(map[string]struct{})
	for _, ns := range namespaces {
		list := &appsv1.DeploymentList{}
		if err := r.List(ctx, list, client.InNamespace(ns)); err != nil {
			return fmt.Errorf("listing deployments in %s: %w", ns, err)
		}
		for _, d := range list.Items {
			existing[fmt.Sprintf("%s/%s", d.Namespace, d.Name)] = struct{}{}
		}
	}

	for key := range policy.Status.Workloads {
		if _, found := existing[key]; !found {
			delete(policy.Status.Workloads, key)
		}
	}

	return nil
}

// ============================================================================
// Condition helpers
// ============================================================================

// setCondition upserts a condition into the conditions slice.
// Uses the standard apimachinery helper which handles LastTransitionTime correctly, it only updates the timestamp when the Status actually changes.
func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) {
	meta.SetStatusCondition(conditions, cond)
}

// removeCondition removes a condition by type.
func removeCondition(conditions *[]metav1.Condition, condType string) {
	meta.RemoveStatusCondition(conditions, condType)
}

// ============================================================================
// Watch mapping, secondary resources to DORAPolicy
// ============================================================================

// mapToDORAPolicy is an event handler that maps a secondary object (Deployment, Namespace) to the DORAPolicy reconcile requests that should be triggered.
//
// Strategy: list all DORAPolicy objects and enqueue any whose observed namespaces include the changed object's namespace.
func (r *DORAPolicyReconciler) mapToDORAPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	policies := &lodestarv1alpha1.DORAPolicyList{}
	if err := r.List(ctx, policies); err != nil {
		return nil
	}

	ns := obj.GetNamespace()
	var requests []reconcile.Request

	for _, policy := range policies.Items {
		if policy.Spec.NamespaceSelector == nil {
			if policy.Namespace == ns {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: policy.Namespace,
						Name:      policy.Name,
					},
				})
			}
			continue
		}

		for _, observed := range policy.Status.ObservedNamespaces {
			if observed == ns {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: policy.Namespace,
						Name:      policy.Name,
					},
				})
				break
			}
		}
	}

	return requests
}

// ============================================================================
// Deployment observation
// ============================================================================

// observeDeployments iterates over all Deployments in the selected namespaces
// and processes any that have completed a new rollout.
func (r *DORAPolicyReconciler) observeDeployments(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
	namespaces []string,
) error {
	log := logf.FromContext(ctx)

	for _, ns := range namespaces {
		list := &appsv1.DeploymentList{}
		if err := r.List(ctx, list, client.InNamespace(ns)); err != nil {
			return fmt.Errorf("listing deployments in %s: %w", ns, err)
		}

		for i := range list.Items {
			d := &list.Items[i]
			if err := r.processDeployment(ctx, policy, d); err != nil {
				// Non-fatal: log and continue to next deployment.
				log.Error(err, "processing deployment",
					"deployment", fmt.Sprintf("%s/%s", d.Namespace, d.Name))
			}
		}
	}

	return nil
}

// processDeployment checks a single Deployment for a completed rollout and
// emits metrics if a new image digest is observed.
func (r *DORAPolicyReconciler) processDeployment(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
	d *appsv1.Deployment,
) error {
	log := logf.FromContext(ctx)
	key := fmt.Sprintf("%s/%s", d.Namespace, d.Name)

	// ── 1. Check if rollout is complete ──────────────────────────────────

	if !deploymentIsComplete(d) {
		return nil
	}

	// ── 2. Load existing workload state ──────────────────────────────────

	if policy.Status.Workloads == nil {
		policy.Status.Workloads = make(map[string]lodestarv1alpha1.WorkloadStatus)
	}
	state := policy.Status.Workloads[key]

	// ── 3. Check if this generation was already processed ────────────────
	//
	// This is the double-counting prevention. If we already processed this
	// generation, skip it — even after operator restarts or informer resyncs.

	if state.ObservedGeneration == d.Generation {
		return nil
	}

	// ── 4. Extract image digest from running Pod ──────────────────────────

	digest, inferred, err := imageDigestForDeployment(
		ctx, r.Client, d, policy.Spec.PrimaryContainer,
	)
	if err != nil {
		return fmt.Errorf("extracting image digest: %w", err)
	}

	// ── 5. Emit PrimaryContainerInferred condition if needed ──────────────

	if inferred {
		setCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               ConditionPrimaryContainerInferred,
			Status:             metav1.ConditionTrue,
			Reason:             "FallbackToIndex0",
			Message:            fmt.Sprintf("workload %s has multiple containers; using index 0", key),
			ObservedGeneration: policy.Generation,
		})
	}

	// ── 6. Check if this is a genuinely new deployment ────────────────────
	//
	// Same digest as last time = config change only, not a new image.
	// We still update ObservedGeneration to prevent reprocessing.

	isNewDeployment := digest != state.LastDigest
	isRollback := state.LastDigest != "" && isNewDeployment &&
		digestSeenBefore(digest, policy.Status.Workloads)

	// ── 7. Update workload state ──────────────────────────────────────────

	now := metav1.Now()
	state.LastDigest = digest
	state.ObservedGeneration = d.Generation
	state.LastCompletedAt = &now
	state.RolloutStartedAt = nil // clear in-progress marker
	policy.Status.Workloads[key] = state

	// ── 8. Emit metrics ───────────────────────────────────────────────────

	if isNewDeployment {
		deploymentsTotal.WithLabelValues(
			d.Namespace,
			d.Name,
			policy.Spec.Environment,
		).Inc()

		log.Info("deployment observed",
			"workload", key,
			"digest", digest,
			"rollback", isRollback,
			"environment", policy.Spec.Environment,
		)

		// Lead Time is only meaningful for forward deployments, not rollbacks —
		// a rollback's "build time" is in the past and would produce a
		// misleadingly large or even negative duration.
		if !isRollback {
			r.observeLeadTime(ctx, policy, d, digest)
		}
	}

	return nil
}

// observeLeadTime computes and emits Lead Time for Changes: the duration
// from image build time to production deployment.
//
// Failures here are non-fatal and logged only — Deployment Frequency must
// never be blocked by registry connectivity issues.
func (r *DORAPolicyReconciler) observeLeadTime(
	ctx context.Context,
	policy *lodestarv1alpha1.DORAPolicy,
	d *appsv1.Deployment,
	digest string,
) {
	log := logf.FromContext(ctx)

	pod, err := runningPodForDeployment(ctx, r.Client, d)
	if err != nil {
		log.Error(err, "finding pod for lead time calculation")
		return
	}

	repository := imageRepository(pod, policy.Spec.PrimaryContainer)
	if repository == "" {
		log.Info("could not determine image repository, skipping lead time")
		return
	}

	meta, err := r.getRegistryClient().GetMetadata(ctx, imageRef{
		Repository:         repository,
		Digest:             digest,
		Namespace:          pod.Namespace,
		ServiceAccountName: pod.Spec.ServiceAccountName,
		ImagePullSecrets:   podPullSecretNames(pod.Spec),
	})
	if err != nil {
		log.Error(err, "fetching image metadata for lead time")
		return
	}

	if meta.CreatedAt == nil {
		log.Info("image has no org.opencontainers.image.created label, skipping lead time",
			"digest", digest)
		return
	}

	leadTime := time.Since(*meta.CreatedAt)
	if leadTime < 0 {
		// Clock skew between build and cluster — do not emit a negative duration.
		log.Info("computed negative lead time, skipping", "digest", digest)
		return
	}

	leadTimeSeconds.WithLabelValues(
		d.Namespace,
		d.Name,
		policy.Spec.Environment,
		"image_label",
	).Observe(leadTime.Seconds())
}

// digestSeenBefore returns true if the given digest matches the LastDigest
// of any other workload tracked by this policy.
// Used to detect rollbacks — a digest reverting to a previously seen value.
func digestSeenBefore(digest string, workloads map[string]lodestarv1alpha1.WorkloadStatus) bool {
	for _, w := range workloads {
		if w.LastDigest == digest {
			return true
		}
	}
	return false
}

// imageRepository returns the image repository (without tag or digest) for
// the primary container of the given Pod.
func imageRepository(pod *corev1.Pod, primaryContainerName string) string {
	idx, _ := resolvePrimaryContainer(pod.Spec, primaryContainerName)
	if idx >= len(pod.Spec.Containers) {
		return ""
	}

	image := pod.Spec.Containers[idx].Image
	// Strip tag or digest suffix to get the bare repository reference.
	if at := strings.LastIndex(image, "@"); at != -1 {
		return image[:at]
	}
	if colon := strings.LastIndex(image, ":"); colon != -1 {
		// Guard against ports in registry hostnames, e.g. localhost:5000/app
		if slash := strings.LastIndex(image, "/"); slash == -1 || colon > slash {
			return image[:colon]
		}
	}
	return image
}

// ============================================================================
// SetupWithManager
// ============================================================================

// SetupWithManager registers the controller with the manager and declares which objects it watches.
func (r *DORAPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lodestarv1alpha1.DORAPolicy{}).
		Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.mapToDORAPolicy),
		).
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapToDORAPolicy),
		).
		Named("dorapolicy").
		Complete(r)
}

// runningPodForDeployment finds a running Pod owned by the Deployment's
// current ReplicaSet. Used by Lead Time calculation to read the Pod's
// imagePullSecrets and ServiceAccountName for registry authentication.
func runningPodForDeployment(
	ctx context.Context,
	c client.Client,
	d *appsv1.Deployment,
) (*corev1.Pod, error) {
	rsList := &appsv1.ReplicaSetList{}
	if err := c.List(ctx, rsList,
		client.InNamespace(d.Namespace),
		client.MatchingLabels(d.Spec.Selector.MatchLabels),
	); err != nil {
		return nil, fmt.Errorf("listing replicasets: %w", err)
	}

	currentRS := currentReplicaSet(d, rsList.Items)
	if currentRS == nil {
		return nil, fmt.Errorf("no current replicaset found for deployment %s/%s",
			d.Namespace, d.Name)
	}

	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(d.Namespace),
		client.MatchingLabels(currentRS.Spec.Selector.MatchLabels),
	); err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}

	pod := runningPod(podList.Items)
	if pod == nil {
		return nil, fmt.Errorf("no running pod found for replicaset %s/%s",
			currentRS.Namespace, currentRS.Name)
	}

	return pod, nil
}
