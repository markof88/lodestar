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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Rollout detection", func() {

	// ── deploymentIsComplete ──────────────────────────────────────────────

	Describe("deploymentIsComplete", func() {
		var (
			d       *appsv1.Deployment
			desired int32 = 3
		)

		BeforeEach(func() {
			d = &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 2,
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &desired,
				},
				Status: appsv1.DeploymentStatus{
					ObservedGeneration:  2,
					UpdatedReplicas:     3,
					AvailableReplicas:   3,
					UnavailableReplicas: 0,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentAvailable,
							Status: corev1.ConditionTrue,
						},
					},
				},
			}
		})

		It("returns true when all five conditions are satisfied", func() {
			Expect(deploymentIsComplete(d)).To(BeTrue())
		})

		It("returns false when ObservedGeneration is behind Generation", func() {
			d.Status.ObservedGeneration = 1
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("returns false when UpdatedReplicas is less than desired", func() {
			d.Status.UpdatedReplicas = 2
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("returns false when AvailableReplicas is less than desired", func() {
			d.Status.AvailableReplicas = 2
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("returns false when UnavailableReplicas is non-zero", func() {
			d.Status.UnavailableReplicas = 1
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("returns false when Available condition is False", func() {
			d.Status.Conditions[0].Status = corev1.ConditionFalse
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("returns false when Available condition is missing", func() {
			d.Status.Conditions = nil
			Expect(deploymentIsComplete(d)).To(BeFalse())
		})

		It("defaults desired replicas to 1 when spec.replicas is nil", func() {
			d.Spec.Replicas = nil
			d.Status.UpdatedReplicas = 1
			d.Status.AvailableReplicas = 1
			Expect(deploymentIsComplete(d)).To(BeTrue())
		})
	})

	// ── resolvePrimaryContainer ───────────────────────────────────────────

	Describe("resolvePrimaryContainer", func() {
		var spec corev1.PodSpec

		BeforeEach(func() {
			spec = corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app"},
					{Name: "istio-proxy"},
				},
			}
		})

		It("uses the named container when primaryContainerName is set", func() {
			idx, inferred := resolvePrimaryContainer(spec, "istio-proxy")
			Expect(idx).To(Equal(1))
			Expect(inferred).To(BeFalse())
		})

		It("uses index 0 with inferred=true when name not found", func() {
			idx, inferred := resolvePrimaryContainer(spec, "nonexistent")
			Expect(idx).To(Equal(0))
			Expect(inferred).To(BeTrue())
		})

		It("uses the single container without inferred when only one exists", func() {
			spec.Containers = []corev1.Container{{Name: "app"}}
			idx, inferred := resolvePrimaryContainer(spec, "")
			Expect(idx).To(Equal(0))
			Expect(inferred).To(BeFalse())
		})

		It("falls back to index 0 with inferred=true for multiple containers and no name", func() {
			idx, inferred := resolvePrimaryContainer(spec, "")
			Expect(idx).To(Equal(0))
			Expect(inferred).To(BeTrue())
		})
	})

	// ── extractDigest ─────────────────────────────────────────────────────

	Describe("extractDigest", func() {
		It("extracts digest from a fully qualified imageID", func() {
			imageID := "docker.io/library/nginx@sha256:abc123def456"
			Expect(extractDigest(imageID)).To(Equal("sha256:abc123def456"))
		})

		It("returns the digest as-is when already a bare digest", func() {
			imageID := "sha256:abc123def456"
			Expect(extractDigest(imageID)).To(Equal("sha256:abc123def456"))
		})

		It("handles private registry imageIDs", func() {
			imageID := "registry.example.com/myapp@sha256:deadbeef"
			Expect(extractDigest(imageID)).To(Equal("sha256:deadbeef"))
		})

		It("returns empty string when no sha256 prefix found", func() {
			Expect(extractDigest("myapp:latest")).To(Equal(""))
		})

		It("returns empty string for empty input", func() {
			Expect(extractDigest("")).To(Equal(""))
		})
	})
})
