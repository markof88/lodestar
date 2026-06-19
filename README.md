# Lodestar

A Kubernetes operator that computes DORA metrics from runtime state.

Most DORA metric tools work by reading your CI/CD pipeline events: what GitHub Actions reported, what Jenkins logged, what ArgoCD says it deployed. Lodestar takes a different approach. It watches your cluster directly and measures what actually ran in production, not what a pipeline claimed happened.

You install one operator, create one `DORAPolicy` resource, and Lodestar starts computing Deployment Frequency, Lead Time for Changes, Change Failure Rate, and MTTR for every workload in the namespaces you point it at. Your application teams never have to add an annotation, install an agent, or change anything about how they deploy.

## Why runtime state

A Deployment's `spec.template.spec.containers[].image` field tells you what someone *wanted* to run. It's often a mutable tag like `myapp:latest`, which can point to a different image entirely from one day to the next. It's not proof anything actually happened.

The Pod that's actually running has a `status.containerStatuses[].imageID` field. This is always a fully resolved digest, something like `sha256:a1b2c3...`. It's set by the kubelet after the image was pulled and the container started. That's the closest thing to ground truth you can get from inside a cluster, and it's what Lodestar reads.

## How it works

```yaml
apiVersion: lodestar.io/v1alpha1
kind: DORAPolicy
metadata:
  name: production
  namespace: lodestar-system
spec:
  environment: production
  namespaceSelector:
    matchLabels:
      environment: production
```

Apply that, and Lodestar starts watching every namespace labeled `environment: production`. For each Deployment it finds, it:

1. Waits for a rollout to fully complete (all replicas updated, available, and passing readiness)
2. Reads the real image digest from the running Pod, not the tag in the spec
3. Compares it against the last digest it saw for that workload
4. If it's new, counts a deployment and tries to work out how long the image sat around before it got here

For that last part it reads the `org.opencontainers.image.created` label baked into the image at build time. Most CI systems set this automatically (GitHub Actions, GitLab CI, and anything using Buildpacks all do it without extra configuration). Lodestar subtracts that timestamp from the deployment time and that's your Lead Time. If the label isn't there, Lodestar doesn't guess, it just skips the metric for that deployment rather than making something up.

Failures are tracked too. Within a configurable window after a deployment (15 minutes by default), Lodestar watches for `CrashLoopBackOff`, `OOMKilled`, a Deployment hitting `ProgressDeadlineExceeded`, or a rollback to a previously seen digest. Any of those opens an incident. The next successful deployment closes it, and the time between is your MTTR.

Everything comes out as Prometheus metrics on the operator's `/metrics` endpoint. There's no separate dashboard to stand up. If you already have Grafana scraping your cluster, point it at Lodestar and build panels with the metrics already there.

## Metrics

| Metric | Type | What it tells you |
|---|---|---|
| `lodestar_deployments_total` | counter | Deployment Frequency, as a rate over time |
| `lodestar_lead_time_seconds` | histogram | Time from image build to running in production |
| `lodestar_failed_deployments_total` | counter | Numerator for Change Failure Rate |
| `lodestar_time_to_restore_seconds` | histogram | MTTR |

All four carry `namespace`, `workload`, and `environment` labels so you can slice by team or service.

## Installing

```bash
kubectl apply -f https://raw.githubusercontent.com/markof88/lodestar/main/config/crd/bases/lodestar.io_dorapolicies.yaml
```

Then deploy the operator (see `config/manager` for the manifest, or use the Helm chart once it lands).

Create a `DORAPolicy` in the namespace where the operator runs, pointing at whichever namespaces you want observed. That's the entire setup.

## Registry authentication

When Lodestar needs to read OCI labels off an image, it reuses whatever credentials the kubelet already had for pulling that image: the Pod's `imagePullSecrets`, the ServiceAccount's pull secrets, or cloud workload identity (IRSA on EKS, Workload Identity on GKE, similar on AKS) if nothing else applies. There's no separate credential to provision for Lodestar itself.

If you ever suspect the cached image metadata has gone stale, you can force a refresh:

```bash
kubectl annotate dorapolicy production lodestar.io/refresh-image-cache=true
```

Lodestar clears its cache on the next reconcile and removes the annotation automatically.

## Configuration reference

```yaml
spec:
  environment: production        # production | staging | development

  namespaceSelector:              # omit to watch only the policy's own namespace
    matchLabels:
      environment: production

  primaryContainer: ""            # name of the container whose digest matters
                                   # for multi-container pods. leave blank and
                                   # Lodestar will pick the only non-sidecar
                                   # container if there's exactly one, otherwise
                                   # it falls back to index 0 and tells you about
                                   # it via a condition

  failureWindow: 15m               # how long after a deploy to watch for trouble

  failureSignals:
    builtIn:                       # omit this whole block to enable all four
      - ProgressDeadlineExceeded
      - CrashLoopBackOff
      - OOMKilled
      - Rollback
```

## What Lodestar deliberately doesn't do

It doesn't read your Git history or your CI logs. If your build pipeline takes two days from commit to image, that shows up in Lead Time, and that's accurate, that's actually how long it took. A future version may support a webhook from your Git provider for teams that want commit-to-production precision instead of build-to-production, but that's additive, not a replacement for what's here now.

It doesn't require ArgoCD, Flux, or any particular GitOps tool. It watches Deployments directly, so it works the same whether you deploy with `kubectl apply`, Helm, or a GitOps controller.

It doesn't ship a UI. Prometheus and whatever you already use to look at Prometheus data is the UI.

## Status

Early. The core metric pipeline works and is tested, but this hasn't run in a real production cluster yet. Treat it accordingly. StatefulSet and DaemonSet support, plus the optional Git webhook for exact Lead Time, are not built yet.

## License

Apache 2.0