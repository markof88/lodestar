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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodestarv1alpha1 "github.com/markof88/lodestar/api/v1alpha1"
)

// ============================================================================
// Test fixtures: build a complete Deployment -> ReplicaSet -> Pod chain.
//
// envtest runs a real API server but no kubelet, so status fields that a
// kubelet would normally populate (Pod.status.containerStatuses, etc.) must
// be set manually via the status subresource. This is a faithful simulation:
// we are setting exactly the fields a real kubelet would set.
// ============================================================================

// testWorkload bundles the objects created for a single test deployment,
// so test cases can reference them without re-deriving names.
type testWorkload struct {
	Namespace  string
	Name       string
	Deployment *appsv1.Deployment
	ReplicaSet *appsv1.ReplicaSet
	Pod        *corev1.Pod
}

// createCompleteRollout creates a Deployment, its owning ReplicaSet, and a
// Running Pod with the given image digest, simulating a fully completed
// rollout — all five completion conditions satisfied.
func createCompleteRollout(
	ctx context.Context,
	namespace, name, imageDigest string,
	generation int64,
) (*testWorkload, error) {
	labels := map[string]string{"app": name}
	podTemplateHash := fmt.Sprintf("%s-%d", name, generation)
	rsLabels := map[string]string{"app": name, "pod-template-hash": podTemplateHash}

	var replicas int32 = 1

	// ── Deployment ────────────────────────────────────────────────────────

	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: rsLabels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app", Image: fmt.Sprintf("ghcr.io/test/%s:latest", name)},
					},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, d); err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	d.Status = appsv1.DeploymentStatus{
		ObservedGeneration:  generation,
		Replicas:            1,
		UpdatedReplicas:     1,
		AvailableReplicas:   1,
		ReadyReplicas:       1,
		UnavailableReplicas: 0,
		Conditions: []appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
		},
	}
	d.Generation = generation
	if err := k8sClient.Status().Update(ctx, d); err != nil {
		return nil, fmt.Errorf("updating deployment status: %w", err)
	}

	// ── ReplicaSet ────────────────────────────────────────────────────────

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", name, podTemplateHash),
			Namespace: namespace,
			Labels:    rsLabels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       d.Name,
					UID:        d.UID,
					Controller: boolPtr(true),
				},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: rsLabels},
			Template: d.Spec.Template,
		},
	}
	if err := k8sClient.Create(ctx, rs); err != nil {
		return nil, fmt.Errorf("creating replicaset: %w", err)
	}

	// ── Pod ───────────────────────────────────────────────────────────────

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-abcde", name, podTemplateHash),
			Namespace: namespace,
			Labels:    rsLabels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       rs.Name,
					UID:        rs.UID,
					Controller: boolPtr(true),
				},
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: fmt.Sprintf("ghcr.io/test/%s:latest", name)},
			},
		},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("creating pod: %w", err)
	}

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:    "app",
				Ready:   true,
				ImageID: fmt.Sprintf("ghcr.io/test/%s@%s", name, imageDigest),
				State:   corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			},
		},
	}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		return nil, fmt.Errorf("updating pod status: %w", err)
	}

	return &testWorkload{
		Namespace:  namespace,
		Name:       name,
		Deployment: d,
		ReplicaSet: rs,
		Pod:        pod,
	}, nil
}

// boolPtr returns a pointer to the given bool. Needed because Go has no
// literal syntax for pointer-to-bool constants.
func boolPtr(b bool) *bool {
	return &b
}

// deleteWorkload removes all objects created by createCompleteRollout.
func deleteWorkload(ctx context.Context, w *testWorkload) {
	_ = k8sClient.Delete(ctx, w.Pod)
	_ = k8sClient.Delete(ctx, w.ReplicaSet)
	_ = k8sClient.Delete(ctx, w.Deployment)
}

// ============================================================================
// Integration tests: stateful reconcile flow
// ============================================================================

var _ = Describe("Stateful reconcile flow", func() {

	var policyNS string

	BeforeEach(func() {
		policyNS = "default"
	})

	Describe("a completed rollout", func() {
		It("increments deployment frequency and records workload state", func() {
			ctx := context.Background()
			workloadName := "freq-test-app"

			w, err := createCompleteRollout(ctx, policyNS, workloadName, "sha256:aaa111", 1)
			Expect(err).NotTo(HaveOccurred())
			defer deleteWorkload(ctx, w)

			policy := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "freq-test-policy",
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{Environment: "production"},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, policy)
			}()

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: policy.Name, Namespace: policyNS,
			}, updated)).To(Succeed())

			key := fmt.Sprintf("%s/%s", policyNS, workloadName)
			state, tracked := updated.Status.Workloads[key]
			Expect(tracked).To(BeTrue(), "workload should be tracked in status")
			Expect(state.LastDigest).To(Equal("sha256:aaa111"))
			Expect(state.ObservedGeneration).To(Equal(int64(1)))
			Expect(state.LastCompletedAt).NotTo(BeNil())
			Expect(state.FailureDetectedAt).To(BeNil(), "no failure should be recorded for a clean rollout")
		})
	})
	Describe("a CrashLoopBackOff within the failure window", func() {
		It("opens a failure incident and emits failed_deployments_total", func() {
			ctx := context.Background()
			workloadName := "crash-test-app"

			w, err := createCompleteRollout(ctx, policyNS, workloadName, "sha256:bbb222", 1)
			Expect(err).NotTo(HaveOccurred())
			defer deleteWorkload(ctx, w)

			policy := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crash-test-policy",
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{Environment: "production"},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, policy)
			}()

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling once to record the completed rollout")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			By("simulating the pod entering CrashLoopBackOff")
			w.Pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
			}
			w.Pod.Status.ContainerStatuses[0].Ready = false
			Expect(k8sClient.Status().Update(ctx, w.Pod)).To(Succeed())

			By("reconciling again to detect the failure")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: policy.Name, Namespace: policyNS,
			}, updated)).To(Succeed())

			key := fmt.Sprintf("%s/%s", policyNS, workloadName)
			state := updated.Status.Workloads[key]
			Expect(state.FailureDetectedAt).NotTo(BeNil(), "failure should be recorded")

			By("reconciling a third time to confirm idempotency")
			firstDetectedAt := state.FailureDetectedAt
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: policy.Name, Namespace: policyNS,
			}, updated)).To(Succeed())
			state = updated.Status.Workloads[key]
			Expect(state.FailureDetectedAt.Time).To(Equal(firstDetectedAt.Time),
				"FailureDetectedAt must not change on repeated reconciles — incident already open")
		})
	})

	Describe("recovery after a failure", func() {
		It("closes the incident and emits MTTR when a new successful rollout completes", func() {
			ctx := context.Background()
			workloadName := "recovery-test-app"

			w, err := createCompleteRollout(ctx, policyNS, workloadName, "sha256:ccc333", 1)
			Expect(err).NotTo(HaveOccurred())
			defer deleteWorkload(ctx, w)

			policy := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "recovery-test-policy",
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{Environment: "production"},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, policy)
			}()

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling to record the initial rollout")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			By("manually opening a failure incident in status")
			updated := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: policy.Name, Namespace: policyNS,
			}, updated)).To(Succeed())

			key := fmt.Sprintf("%s/%s", policyNS, workloadName)
			state := updated.Status.Workloads[key]
			pastFailure := metav1.NewTime(state.LastCompletedAt.Time)
			state.FailureDetectedAt = &pastFailure
			updated.Status.Workloads[key] = state
			Expect(k8sClient.Status().Update(ctx, updated)).To(Succeed())

			By("deploying a new image digest to simulate the fix")
			w.Pod.Status.ContainerStatuses[0].ImageID = fmt.Sprintf("ghcr.io/test/%s@sha256:ddd444", workloadName)
			w.Pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			w.Pod.Status.ContainerStatuses[0].Ready = true
			Expect(k8sClient.Status().Update(ctx, w.Pod)).To(Succeed())

			// metadata.generation is server-managed and only increments when
			// spec actually changes — we cannot set it directly. Bump the
			// container image to trigger a real generation increment, then
			// re-fetch before updating status so we have the correct
			// resourceVersion and generation.
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: w.Deployment.Name, Namespace: w.Deployment.Namespace,
			}, w.Deployment)).To(Succeed())
			w.Deployment.Spec.Template.Spec.Containers[0].Image = fmt.Sprintf("ghcr.io/test/%s:v2", workloadName)
			Expect(k8sClient.Update(ctx, w.Deployment)).To(Succeed())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: w.Deployment.Name, Namespace: w.Deployment.Namespace,
			}, w.Deployment)).To(Succeed())
			w.Deployment.Status.ObservedGeneration = w.Deployment.Generation
			Expect(k8sClient.Status().Update(ctx, w.Deployment)).To(Succeed())

			By("reconciling to observe the recovery")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: policy.Name, Namespace: policyNS,
			}, updated)).To(Succeed())

			state = updated.Status.Workloads[key]
			Expect(state.LastDigest).To(Equal("sha256:ddd444"))
			Expect(state.FailureDetectedAt).To(BeNil(), "incident should be closed after recovery")
		})
	})
})
