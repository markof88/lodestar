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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodestarv1alpha1 "github.com/markof88/lodestar/api/v1alpha1"
)

var _ = Describe("Failure detection", func() {

	// ── deploymentProgressDeadlineExceeded ─────────────────────────────────

	Describe("deploymentProgressDeadlineExceeded", func() {
		It("returns true when Progressing condition has reason ProgressDeadlineExceeded", func() {
			d := &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionFalse,
							Reason: "ProgressDeadlineExceeded",
						},
					},
				},
			}
			Expect(deploymentProgressDeadlineExceeded(d)).To(BeTrue())
		})

		It("returns false when Progressing condition is True", func() {
			d := &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
							Reason: "NewReplicaSetAvailable",
						},
					},
				},
			}
			Expect(deploymentProgressDeadlineExceeded(d)).To(BeFalse())
		})

		It("returns false when no Progressing condition exists", func() {
			d := &appsv1.Deployment{}
			Expect(deploymentProgressDeadlineExceeded(d)).To(BeFalse())
		})
	})

	// ── podCrashLoopBackOff ──────────────────────────────────────────────

	Describe("podCrashLoopBackOff", func() {
		It("returns true when a container is waiting with reason CrashLoopBackOff", func() {
			pod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
							},
						},
					},
				},
			}
			Expect(podCrashLoopBackOff(pod)).To(BeTrue())
		})

		It("returns false when container is running normally", func() {
			pod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			}
			Expect(podCrashLoopBackOff(pod)).To(BeFalse())
		})

		It("returns false for an empty pod status", func() {
			pod := &corev1.Pod{}
			Expect(podCrashLoopBackOff(pod)).To(BeFalse())
		})
	})

	// ── podOOMKilled ─────────────────────────────────────────────────────

	Describe("podOOMKilled", func() {
		It("returns true when last termination reason was OOMKilled", func() {
			pod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
							},
						},
					},
				},
			}
			Expect(podOOMKilled(pod)).To(BeTrue())
		})

		It("returns true when current state is terminated with OOMKilled", func() {
			pod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"},
							},
						},
					},
				},
			}
			Expect(podOOMKilled(pod)).To(BeTrue())
		})

		It("returns false when terminated for a different reason", func() {
			pod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"},
							},
						},
					},
				},
			}
			Expect(podOOMKilled(pod)).To(BeFalse())
		})

		It("returns false for an empty pod status", func() {
			pod := &corev1.Pod{}
			Expect(podOOMKilled(pod)).To(BeFalse())
		})
	})

	// ── enabledSignals ───────────────────────────────────────────────────

	Describe("enabledSignals", func() {
		It("returns all four built-in signals when FailureSignals is nil", func() {
			policy := &lodestarv1alpha1.DORAPolicy{}
			enabled := enabledSignals(policy)

			Expect(enabled).To(HaveLen(4))
			Expect(enabled[lodestarv1alpha1.SignalProgressDeadlineExceeded]).To(BeTrue())
			Expect(enabled[lodestarv1alpha1.SignalCrashLoopBackOff]).To(BeTrue())
			Expect(enabled[lodestarv1alpha1.SignalOOMKilled]).To(BeTrue())
			Expect(enabled[lodestarv1alpha1.SignalRollback]).To(BeTrue())
		})

		It("returns only the configured signals when BuiltIn is set", func() {
			policy := &lodestarv1alpha1.DORAPolicy{
				Spec: lodestarv1alpha1.DORAPolicySpec{
					FailureSignals: &lodestarv1alpha1.FailureSignalsSpec{
						BuiltIn: []lodestarv1alpha1.BuiltInFailureSignal{
							lodestarv1alpha1.SignalRollback,
						},
					},
				},
			}
			enabled := enabledSignals(policy)

			Expect(enabled).To(HaveLen(1))
			Expect(enabled[lodestarv1alpha1.SignalRollback]).To(BeTrue())
			Expect(enabled[lodestarv1alpha1.SignalCrashLoopBackOff]).To(BeFalse())
		})

		It("returns all four when FailureSignals is set but BuiltIn is empty", func() {
			policy := &lodestarv1alpha1.DORAPolicy{
				Spec: lodestarv1alpha1.DORAPolicySpec{
					FailureSignals: &lodestarv1alpha1.FailureSignalsSpec{},
				},
			}
			enabled := enabledSignals(policy)
			Expect(enabled).To(HaveLen(4))
		})
	})

	// ── failureWindowDuration ────────────────────────────────────────────

	Describe("failureWindowDuration", func() {
		It("defaults to 15 minutes when FailureWindow is nil", func() {
			policy := &lodestarv1alpha1.DORAPolicy{}
			Expect(failureWindowDuration(policy)).To(Equal(15 * time.Minute))
		})

		It("uses the configured duration when set", func() {
			policy := &lodestarv1alpha1.DORAPolicy{
				Spec: lodestarv1alpha1.DORAPolicySpec{
					FailureWindow: &metav1.Duration{Duration: 30 * time.Minute},
				},
			}
			Expect(failureWindowDuration(policy)).To(Equal(30 * time.Minute))
		})
	})
})
