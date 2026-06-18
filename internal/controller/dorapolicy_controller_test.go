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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodestarv1alpha1 "github.com/markof88/lodestar/api/v1alpha1"
)

const (
	timeout  = 10 * time.Second
	interval = 250 * time.Millisecond
)

var _ = Describe("DORAPolicy Controller", func() {

	ctx := context.Background()

	createPolicy := func(name, namespace, environment string) *lodestarv1alpha1.DORAPolicy {
		policy := &lodestarv1alpha1.DORAPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: lodestarv1alpha1.DORAPolicySpec{
				Environment: environment,
			},
		}
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		return policy
	}

	deletePolicy := func(name, namespace string) {
		policy := &lodestarv1alpha1.DORAPolicy{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, policy)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(k8sClient.Delete(ctx, policy)).To(Succeed())
	}

	// ── Test: basic reconciliation ────────────────────────────────────────

	Context("when a DORAPolicy is created", func() {
		const (
			policyName = "test-production"
			policyNS   = "default"
		)

		AfterEach(func() {
			deletePolicy(policyName, policyNS)
		})

		It("should set the Ready condition to True", func() {
			By("creating a DORAPolicy with environment=production")
			createPolicy(policyName, policyNS, "production")

			By("reconciling the resource")
			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      policyName,
					Namespace: policyNS,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the Ready condition is True")
			policy := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      policyName,
				Namespace: policyNS,
			}, policy)).To(Succeed())

			readyCondition := findCondition(policy.Status.Conditions, ConditionReady)
			Expect(readyCondition).NotTo(BeNil(), "Ready condition should exist")
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.Reason).To(Equal("Reconciled"))
		})

		It("should set observedNamespaces to the policy's own namespace when no selector", func() {
			By("creating a DORAPolicy with no namespace selector")
			createPolicy(policyName, policyNS, "production")

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      policyName,
					Namespace: policyNS,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking observedNamespaces contains only the policy's namespace")
			policy := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      policyName,
				Namespace: policyNS,
			}, policy)).To(Succeed())

			Expect(policy.Status.ObservedNamespaces).To(ConsistOf(policyNS))
		})
	})

	// ── Test: namespace selector ──────────────────────────────────────────

	Context("when a DORAPolicy has a namespace selector", func() {
		const (
			policyName = "test-selector"
			policyNS   = "default"
			targetNS   = "test-target-ns"
		)

		AfterEach(func() {
			deletePolicy(policyName, policyNS)
			ns := &corev1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: targetNS}, ns); err == nil {
				Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
			}
		})

		It("should observe namespaces matching the selector", func() {
			By("creating a target namespace with a label")
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   targetNS,
					Labels: map[string]string{"environment": "production"},
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())

			By("creating a DORAPolicy with a namespace selector")
			policy := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      policyName,
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{
					Environment: "production",
					NamespaceSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"environment": "production"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, policy)).To(Succeed())

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      policyName,
					Namespace: policyNS,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking observedNamespaces contains the target namespace")
			updated := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      policyName,
				Namespace: policyNS,
			}, updated)).To(Succeed())

			Expect(updated.Status.ObservedNamespaces).To(ContainElement(targetNS))
		})
	})

	// ── Test: conflict detection ──────────────────────────────────────────

	Context("when two policies select the same namespace", func() {
		const (
			olderPolicy = "older-policy"
			newerPolicy = "newer-policy"
			policyNS    = "default"
		)

		AfterEach(func() {
			deletePolicy(olderPolicy, policyNS)
			deletePolicy(newerPolicy, policyNS)
		})

		It("should set Conflict condition on the losing policy", func() {
			By("creating both policies")
			older := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      olderPolicy,
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{Environment: "production"},
			}
			Expect(k8sClient.Create(ctx, older)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: olderPolicy, Namespace: policyNS,
			}, older)).To(Succeed())

			newer := &lodestarv1alpha1.DORAPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      newerPolicy,
					Namespace: policyNS,
				},
				Spec: lodestarv1alpha1.DORAPolicySpec{Environment: "production"},
			}
			Expect(k8sClient.Create(ctx, newer)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: newerPolicy, Namespace: policyNS,
			}, newer)).To(Succeed())

			// Determine which policy wins (smaller UID wins the tiebreak).
			// The winner gets reconciled first to establish ObservedNamespaces.
			// The loser is reconciled second and should get a Conflict condition.
			winnerName := olderPolicy
			loserName := newerPolicy
			if string(older.UID) > string(newer.UID) {
				winnerName = newerPolicy
				loserName = olderPolicy
			}

			reconciler := &DORAPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling the winner first")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: winnerName, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			winner := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: winnerName, Namespace: policyNS,
			}, winner)).To(Succeed())
			Expect(winner.Status.ObservedNamespaces).To(ContainElement(policyNS),
				"winner must have ObservedNamespaces set before conflict detection")

			By("reconciling the loser")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: loserName, Namespace: policyNS},
			})
			Expect(err).NotTo(HaveOccurred())

			By("checking the loser has a Conflict condition")
			loser := &lodestarv1alpha1.DORAPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: loserName, Namespace: policyNS,
			}, loser)).To(Succeed())

			conflictCondition := findCondition(loser.Status.Conditions, ConditionConflict)
			Expect(conflictCondition).NotTo(BeNil(), "Conflict condition should exist")
			Expect(conflictCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(conflictCondition.Reason).To(Equal("NamespaceAlreadySelected"))
		})
	})
}) // end Describe

// findCondition returns the condition with the given type, or nil if not found.
func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
