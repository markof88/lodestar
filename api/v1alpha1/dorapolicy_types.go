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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ============================================================================
// Spec
// ============================================================================

// DORAPolicySpec defines the desired state of a DORAPolicy.
type DORAPolicySpec struct {
	// Environment labels this policy's tier. Used as a label on all emitted
	// Prometheus metrics so dashboards can filter by environment.
	//
	// +kubebuilder:validation:Enum=production;staging;development
	// +kubebuilder:default=production
	Environment string `json:"environment"`

	// NamespaceSelector selects which namespaces this policy observes.
	// When nil, the policy observes only the namespace it lives in.
	//
	// Two policies may not select overlapping namespaces — the controller
	// emits a Conflict condition on the newer policy.
	//
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// PrimaryContainer is the name of the container whose image digest
	// identifies the deployment for metric purposes.
	//
	// Resolution order when empty:
	//   1. Single non-init container, if exactly one exists.
	//   2. Container index 0 — emits PrimaryContainerInferred condition.
	//
	// Set explicitly in service mesh clusters where index 0 may be a sidecar.
	//
	// +optional
	PrimaryContainer string `json:"primaryContainer,omitempty"`

	// FailureWindow is how long after a successful rollout Lodestar watches
	// for failure signals. Uses metav1.Duration so it serialises as "15m"
	// in YAML rather than a nanosecond integer.
	//
	// +optional
	// +kubebuilder:default="15m"
	FailureWindow *metav1.Duration `json:"failureWindow,omitempty"`

	// FailureSignals configures which signals count as a failed deployment.
	// When nil, all four built-in signals are enabled.
	//
	// +optional
	FailureSignals *FailureSignalsSpec `json:"failureSignals,omitempty"`
}

// FailureSignalsSpec configures which signals Lodestar treats as a failed
// deployment when evaluating Change Failure Rate.
type FailureSignalsSpec struct {
	// BuiltIn lists which built-in signals are active.
	// When empty, all four are enabled.
	//
	// +optional
	// +listType=set
	BuiltIn []BuiltInFailureSignal `json:"builtIn,omitempty"`
}

// BuiltInFailureSignal is a signal Lodestar can detect without external
// dependencies.
type BuiltInFailureSignal string

const (
	// SignalProgressDeadlineExceeded fires when a Deployment's
	// ProgressDeadlineExceeded condition becomes True.
	SignalProgressDeadlineExceeded BuiltInFailureSignal = "ProgressDeadlineExceeded"

	// SignalCrashLoopBackOff fires when any Pod owned by a completed
	// rollout enters CrashLoopBackOff within the failure window.
	SignalCrashLoopBackOff BuiltInFailureSignal = "CrashLoopBackOff"

	// SignalOOMKilled fires when any container is OOMKilled within the
	// failure window.
	SignalOOMKilled BuiltInFailureSignal = "OOMKilled"

	// SignalRollback fires when the observed image digest reverts to a
	// previously seen digest. Treated as a failure because it means the
	// forward deployment did not hold.
	SignalRollback BuiltInFailureSignal = "Rollback"
)

// ============================================================================
// Status
// ============================================================================

// DORAPolicyStatus is the observed state of a DORAPolicy.
// The controller writes here. Platform teams read here.
type DORAPolicyStatus struct {
	// Conditions summarise the current state of this policy.
	//
	// Known condition types:
	//   Ready                    — policy is active and observing workloads
	//   Conflict                 — overlaps another policy's namespaces
	//   PrimaryContainerInferred — container selection fell back to index 0
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedNamespaces lists the namespaces currently selected by this
	// policy. Useful for debugging namespace selector conflicts.
	//
	// +optional
	// +listType=set
	ObservedNamespaces []string `json:"observedNamespaces,omitempty"`

	// Workloads holds compact per-workload state. This is Lodestar's durable
	// store — it survives operator restarts and prevents double-counting.
	//
	// Key format: "<namespace>/<name>"
	// Entries are garbage-collected when the workload no longer exists.
	//
	// +optional
	Workloads map[string]WorkloadStatus `json:"workloads,omitempty"`
}

// WorkloadStatus holds the last-known state of a single observed workload.
// Kept intentionally compact — only what the controller needs to be
// correct after a restart.
type WorkloadStatus struct {
	// LastDigest is the image digest of the primary container from the
	// most recently completed rollout. Format: "sha256:<64 hex chars>"
	//
	// +optional
	LastDigest string `json:"lastDigest,omitempty"`

	// ObservedGeneration is the Deployment generation of the last completed
	// rollout. Used with LastDigest to detect spurious reconciles.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// RolloutStartedAt is when the current in-progress rollout was first
	// observed. Used to compute Lead Time for Changes.
	// Nil when no rollout is in progress.
	//
	// +optional
	RolloutStartedAt *metav1.Time `json:"rolloutStartedAt,omitempty"`

	// LastCompletedAt is when the most recent rollout completed.
	// Used for Deployment Frequency calculations.
	//
	// +optional
	LastCompletedAt *metav1.Time `json:"lastCompletedAt,omitempty"`

	// FailureDetectedAt is when the first failure signal was observed after
	// the most recent rollout. Nil when no active incident exists.
	//
	// +optional
	FailureDetectedAt *metav1.Time `json:"failureDetectedAt,omitempty"`
}

// ============================================================================
// Root types
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dp
// +kubebuilder:printcolumn:name="Environment",type=string,JSONPath=`.spec.environment`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Namespaces",type=string,JSONPath=`.status.observedNamespaces`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DORAPolicy is the Schema for the dorapolicies API.
//
// Platform teams create one DORAPolicy per environment tier. Lodestar
// automatically computes DORA metrics for all workloads in the selected
// namespaces. App teams never interact with this resource.
type DORAPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec DORAPolicySpec `json:"spec"`

	// +optional
	Status DORAPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DORAPolicyList contains a list of DORAPolicy.
type DORAPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DORAPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DORAPolicy{}, &DORAPolicyList{})
		return nil
	})
}
