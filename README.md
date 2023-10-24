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

## Background

This was created after some bugs in the [upstream operator](https://github.com/dask/dask-kubernetes),
which were difficult to resolve without a complete re-write. So, here is the re-write!
