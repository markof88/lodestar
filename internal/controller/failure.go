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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodestarv1alpha1 "github.com/markof88/lodestar/api/v1alpha1"
)

// ============================================================================
// Failure signal detection
// ============================================================================

const (
	reasonCrashLoopBackOff = "CrashLoopBackOff"
	reasonOOMKilled        = "OOMKilled"
)

// detectedFailure describes a single failure signal found on a workload.
type detectedFailure struct {
	Signal  lodestarv1alpha1.BuiltInFailureSignal
	Reason  string
	Message string
}

// defaultFailureSignals returns all four built-in signals, used when
// spec.failureSignals is nil (the default — all signals enabled).
func defaultFailureSignals() []lodestarv1alpha1.BuiltInFailureSignal {
	return []lodestarv1alpha1.BuiltInFailureSignal{
		lodestarv1alpha1.SignalProgressDeadlineExceeded,
		lodestarv1alpha1.SignalCrashLoopBackOff,
		lodestarv1alpha1.SignalOOMKilled,
		lodestarv1alpha1.SignalRollback,
	}
}

// enabledSignals returns the set of failure signals active for a policy.
func enabledSignals(policy *lodestarv1alpha1.DORAPolicy) map[lodestarv1alpha1.BuiltInFailureSignal]bool {
	var signals []lodestarv1alpha1.BuiltInFailureSignal
	if policy.Spec.FailureSignals == nil || len(policy.Spec.FailureSignals.BuiltIn) == 0 {
		signals = defaultFailureSignals()
	} else {
		signals = policy.Spec.FailureSignals.BuiltIn
	}

	enabled := make(map[lodestarv1alpha1.BuiltInFailureSignal]bool, len(signals))
	for _, s := range signals {
		enabled[s] = true
	}
	return enabled
}

// failureWindowDuration returns the configured failure window, defaulting
// to 15 minutes when unset.
func failureWindowDuration(policy *lodestarv1alpha1.DORAPolicy) time.Duration {
	if policy.Spec.FailureWindow == nil {
		return 15 * time.Minute
	}
	return policy.Spec.FailureWindow.Duration
}

// deploymentProgressDeadlineExceeded returns true when the Deployment's
// ProgressDeadlineExceeded condition is currently True.
func deploymentProgressDeadlineExceeded(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing &&
			c.Reason == "ProgressDeadlineExceeded" &&
			c.Status == corev1.ConditionFalse {
			return true
		}
	}
	return false
}

// podCrashLoopBackOff returns true if any container in the Pod is currently
// in CrashLoopBackOff.
func podCrashLoopBackOff(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == reasonCrashLoopBackOff {
			return true
		}
	}
	return false
}

// podOOMKilled returns true if any container in the Pod was last terminated
// due to an out-of-memory kill.
func podOOMKilled(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.LastTerminationState.Terminated != nil &&
			cs.LastTerminationState.Terminated.Reason == reasonOOMKilled {
			return true
		}
		// Also check the current state in case the container is actively
		// terminating with OOMKilled rather than already restarted.
		if cs.State.Terminated != nil &&
			cs.State.Terminated.Reason == reasonOOMKilled {
			return true
		}
	}
	return false
}

// detectFailures checks all pods owned by the Deployment's current
// ReplicaSet for failure signals, returning every signal found.
//
// Multiple signals can be detected simultaneously (e.g. a Pod that is both
// CrashLoopBackOff and was previously OOMKilled) — the caller decides how
// to record them.
func detectFailures(
	ctx context.Context,
	c client.Client,
	d *appsv1.Deployment,
	enabled map[lodestarv1alpha1.BuiltInFailureSignal]bool,
) ([]detectedFailure, error) {
	var failures []detectedFailure

	if enabled[lodestarv1alpha1.SignalProgressDeadlineExceeded] &&
		deploymentProgressDeadlineExceeded(d) {
		failures = append(failures, detectedFailure{
			Signal:  lodestarv1alpha1.SignalProgressDeadlineExceeded,
			Reason:  "ProgressDeadlineExceeded",
			Message: fmt.Sprintf("deployment %s/%s exceeded its progress deadline", d.Namespace, d.Name),
		})
	}

	needsPodCheck := enabled[lodestarv1alpha1.SignalCrashLoopBackOff] ||
		enabled[lodestarv1alpha1.SignalOOMKilled]

	if needsPodCheck {
		rsList := &appsv1.ReplicaSetList{}
		if err := c.List(ctx, rsList,
			client.InNamespace(d.Namespace),
			client.MatchingLabels(d.Spec.Selector.MatchLabels),
		); err != nil {
			return nil, fmt.Errorf("listing replicasets: %w", err)
		}

		currentRS := currentReplicaSet(d, rsList.Items)
		if currentRS == nil {
			return failures, nil
		}

		podList := &corev1.PodList{}
		if err := c.List(ctx, podList,
			client.InNamespace(d.Namespace),
			client.MatchingLabels(currentRS.Spec.Selector.MatchLabels),
		); err != nil {
			return nil, fmt.Errorf("listing pods: %w", err)
		}

		for i := range podList.Items {
			pod := &podList.Items[i]

			if enabled[lodestarv1alpha1.SignalCrashLoopBackOff] && podCrashLoopBackOff(pod) {
				failures = append(failures, detectedFailure{
					Signal:  lodestarv1alpha1.SignalCrashLoopBackOff,
					Reason:  reasonCrashLoopBackOff,
					Message: fmt.Sprintf("pod %s/%s is in CrashLoopBackOff", pod.Namespace, pod.Name),
				})
			}

			if enabled[lodestarv1alpha1.SignalOOMKilled] && podOOMKilled(pod) {
				failures = append(failures, detectedFailure{
					Signal:  lodestarv1alpha1.SignalOOMKilled,
					Reason:  reasonOOMKilled,
					Message: fmt.Sprintf("pod %s/%s was OOMKilled", pod.Namespace, pod.Name),
				})
			}
		}
	}

	return failures, nil
}
