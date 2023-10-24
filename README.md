# dask-operator

A Kubernetes operator to deploy [Dask](https://www.dask.org/)https://www.dask.org/ clusters.

## Installation

Apply the manifests in the `deploy/` directory. Note this deploys into the
current namespace, to deploy into a custom namespace, create the namespace and
add a `namespace: ...` field to `kustomization..yaml`.

To create a test cluster, apply the manifests in the `example/` directory. The
cluster can be scaled with (change `--replicas` as desired):

```bash
kubectl scale clusters.dask.charmtx.com/example-dask --replicas=2
```

### Autoscaling

This implementation supports the Horizontal Pod Autoscaler, with the caveat that
scaling to zero requires enabling an alpha feature-gate (see
https://github.com/kubernetes/enhancements/issues/2021). This requires a few
additional steps to expose the desired pods to the autoscaler.

The recommended solution is using the [Prometheus Operator] and [Prometheus
Adapter].

First, this requires that your scheduler pod has the `prometheus_client` library
installed, otherwise Dask does not run the metrics endpoint.

Second, you need to scrape the metrics with a `ServiceMonitor`:

```yaml
apiVersion: monitoring.coreos.com/v1,
kind: "ServiceMonitor",
metadata: { name: dask-scheduler },
spec:
    selector: { matchLabels: { "dask.charmtx.com/role": "scheduler" } }
    jobLabel: dask.charmtx.com/cluster
    endpoints: [{ port: "http-dashboard" }]
    namespaceSelector: { any: true }
```

Finally, you need to configure the prometheus adapter to serve the
`desired_workers` metric, by adding the following item to the adapters `rules`:

```yaml
- seriesQuery: dask_scheduler_desired_workers{namespace!="",service!=""}
  name:
    matches: ^(dask)_scheduler_(.*)$
    as: $1_$2
  resources:
    overrides:
      job: { group: dask.charmtx.com, resource: cluster }
      namespace: { resource: namespace }
  metricsQuery: sum(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)
```

## Background

This was created after some bugs in the [upstream operator](https://github.com/dask/dask-kubernetes),
which were difficult to resolve without a complete re-write. So, here is the re-write!

[Prometheus Operator]: https://github.com/prometheus-operator/prometheus-operator
[Prometheus Adapter]: https://github.com/kubernetes-sigs/prometheus-adapter
