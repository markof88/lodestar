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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Lodestar emits four DORA metrics as Prometheus counters and histograms.
//
// Naming follows the Prometheus convention:
//   lodestar_<metric>_<unit>
//
// All metrics carry these labels:
//   namespace   — Kubernetes namespace of the workload
//   workload    — Deployment name
//   environment — from DORAPolicy.spec.environment

var (
	// deploymentsTotal counts completed rollouts where the image digest changed.
	// This is the raw counter behind Deployment Frequency.
	// Rate is computed in Grafana: rate(lodestar_deployments_total[1d])
	deploymentsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lodestar_deployments_total",
			Help: "Total number of successful deployments observed by Lodestar. " +
				"A deployment is counted when a rollout completes with a new image digest.",
		},
		[]string{"namespace", "workload", "environment"},
	)

	// leadTimeSeconds records the duration from image build to production.
	// Sourced from org.opencontainers.image.created OCI label.
	// Phase 3 adds source="git_webhook" for exact commit timestamps.
	leadTimeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "lodestar_lead_time_seconds",
			Help: "Time from image build to successful production deployment, in seconds. " +
				"Sourced from the org.opencontainers.image.created OCI label.",
			// Buckets cover 1 minute to 7 days — typical CI/CD lead times.
			Buckets: []float64{
				60,     // 1 minute
				300,    // 5 minutes
				900,    // 15 minutes
				1800,   // 30 minutes
				3600,   // 1 hour
				7200,   // 2 hours
				14400,  // 4 hours
				28800,  // 8 hours
				86400,  // 1 day
				259200, // 3 days
				604800, // 7 days
			},
		},
		[]string{"namespace", "workload", "environment", "source"},
	)

	// changeFailureRate tracks whether a deployment was followed by a failure signal.
	// numerator: lodestar_failed_deployments_total
	// denominator: lodestar_deployments_total
	// CFR = rate(failed) / rate(total) in Grafana
	failedDeploymentsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lodestar_failed_deployments_total",
			Help: "Total number of deployments followed by a failure signal within the failure window.",
		},
		[]string{"namespace", "workload", "environment", "reason"},
	)

	// timeToRestoreSeconds records MTTR — time from first failure signal to recovery.
	// Recovery means a new successful rollout completed after the failure.
	timeToRestoreSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "lodestar_time_to_restore_seconds",
			Help: "Time from first failure signal to next successful deployment, in seconds.",
			// Buckets cover 1 minute to 7 days.
			Buckets: []float64{
				60,
				300,
				900,
				1800,
				3600,
				7200,
				28800,
				86400,
				259200,
				604800,
			},
		},
		[]string{"namespace", "workload", "environment"},
	)
)

func init() {
	// Register all metrics with the controller-runtime metrics registry.
	// This makes them available on the /metrics endpoint automatically.
	metrics.Registry.MustRegister(
		deploymentsTotal,
		leadTimeSeconds,
		failedDeploymentsTotal,
		timeToRestoreSeconds,
	)
}
