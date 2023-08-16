import asyncio
import os

import kopf

from kubernetes_asyncio.config import (
    load_kube_config,
    load_incluster_config,
    ConfigException,
)
from kubernetes_asyncio.client import Configuration, ApiClient
from kubernetes_asyncio import client

from . import templates


@kopf.on.login()
async def login(memo: kopf.Memo, **kwargs):
    config = Configuration()
    try:
        await load_incluster_config(client_configuration=config)
    except ConfigException:
        await load_kube_config(os.environ["KUBECONFIG"], client_configuration=config)
    conn = kopf.login_with_service_account(**kwargs) or kopf.login_with_kubeconfig(
        **kwargs
    )
    memo["api"] = ApiClient(configuration=config)
    return conn


@kopf.on.cleanup()
async def logout(memo: kopf.Memo, **kwargs):
    await memo["api"].close()


@kopf.on.create("dask.charmtx.com", "clusters")
async def scheduler(
    name: str, namespace: str, spec: kopf.Spec, memo: kopf.Memo, **kwargs
):
    v1 = client.CoreV1Api(memo["api"])
    appsv1 = client.AppsV1Api(memo["api"])

    cluster_name, name = name, f"{name}-scheduler"
    labels = {
        "dask.charmtx.com/cluster": cluster_name,
        "dask.charmtx.com/role": "scheduler",
    }
    replicaset = {
        "metadata": {"name": name, "labels": labels},
        "spec": client.V1ReplicaSetSpec(
            replicas=1,
            selector={"matchLabels": labels},
            template=templates.scheduler_template(
                spec["scheduler"]["template"], labels
            ),
        ),
    }
    kopf.adopt(replicaset)

    service = {
        "metadata": {"name": name, "labels": labels},
        "spec": templates.scheduler_service(
            spec["scheduler"].get("service", {}), labels
        ),
    }
    kopf.adopt(service)

    port = templates.get(service["spec"].ports, "tcp-comm")["port"]
    address = f"tcp://{name}.{namespace}.svc:{port}"
    await asyncio.gather(
        appsv1.create_namespaced_replica_set(namespace=namespace, body=replicaset),
        v1.create_namespaced_service(namespace=namespace, body=service),
    )

    return {"address": address}


@kopf.index("pods", labels={"dask.charmtx.com/role": "worker"})
async def worker_pods(namespace: str, name: str, labels: kopf.Labels, **kwargs):
    cluster_name = labels["dask.charmtx.com/cluster"]
    return {(namespace, cluster_name): name}


@kopf.on.update(
    "v1",
    "pods",
    labels={"dask.charmtx.com/role": "worker"},
    field="status.phase",
    new="Succeeded",
)
async def clean_completed(namespace: str, name: str, memo: kopf.Memo, **kwargs):
    v1 = client.CoreV1Api(memo["api"])
    await v1.delete_namespaced_pod(name, namespace)


def is_deletion_event(event: kopf.RawEvent, **kwargs):
    return event["type"] == "DELETED"


@kopf.on.event(
    "v1",
    "pods",
    labels={
        "dask.charmtx.com/role": "worker",
        "dask.charmtx.com/cluster": kopf.PRESENT,
    },
    when=is_deletion_event,
)
async def delete_worker(
    namespace: str,
    labels: kopf.Labels,
    memo: kopf.Memo,
    worker_pods: kopf.Index,
    **kwargs,
):
    custom = client.CustomObjectsApi(memo["api"])

    cluster = labels["dask.charmtx.com/cluster"]
    existing_workers = worker_pods.get((namespace, cluster), [])

    # There is technically a race condition here, as both the cluster handler
    # and pod handler write to this condition. But both update the field with
    # the length of `worker_pods`, so it will eventually stabilize.
    await custom.patch_namespaced_custom_object_status(
        "dask.charmtx.com",
        "v1alpha1",
        namespace,
        "clusters",
        cluster,
        {"status": {"workers": {"count": len(existing_workers)}}},
        _content_type="application/merge-patch+json",
    )


@kopf.on.create("dask.charmtx.com", "clusters")
@kopf.on.update("dask.charmtx.com", "clusters", field="spec.worker.replicas")
@kopf.on.update("dask.charmtx.com", "clusters", field="status.workers.count")
async def workers(
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    memo: kopf.Memo,
    worker_pods: kopf.Index,
    **kwargs,
):
    v1 = client.CoreV1Api(memo["api"])

    labels = {
        "dask.charmtx.com/cluster": name,
        "dask.charmtx.com/role": "worker",
    }
    worker_metadata = {"generateName": f"{name}-worker-", "labels": labels}
    if not status.get("scheduler"):
        raise kopf.TemporaryError(
            f"Scheduler not created yet for {namespace}/{name}", delay=1
        )

    existing_workers = len(worker_pods.get((namespace, name), []))
    required_workers = max(spec["worker"]["replicas"] - existing_workers, 0)

    worker_template = templates.worker_template(
        spec["worker"]["template"], worker_metadata, status["scheduler"]["address"]
    )
    kopf.adopt(worker_template)
    await asyncio.gather(
        *(
            v1.create_namespaced_pod(namespace, worker_template)
            for _ in range(required_workers)
        )
    )
    return {"count": existing_workers + required_workers}
