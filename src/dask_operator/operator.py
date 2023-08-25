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


@kopf.index("dask.charmtx.com", "clusters")
async def cluster_queues(
    namespace: str, name: str, logger: kopf.Logger, cluster_queues: kopf.Index, **kwargs
):
    if (namespace, name) not in cluster_queues:
        logger.info(f"Creating new queue for cluster")
        return {(namespace, name): (set(), asyncio.Lock())}


@kopf.index("pods", labels={"dask.charmtx.com/role": "worker"})
async def worker_pods(
    namespace: str, name: str, labels: kopf.Labels, logger: kopf.Logger, **kwargs
):
    cluster_name = labels["dask.charmtx.com/cluster"]
    logger.info(f"Adding pod to cluster {namespace}/{name}")
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


@kopf.on.create(
    "v1",
    "pods",
    labels={
        "dask.charmtx.com/role": "worker",
        "dask.charmtx.com/cluster": kopf.PRESENT,
    },
)
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
    name: str,
    namespace: str,
    labels: kopf.Labels,
    memo: kopf.Memo,
    worker_pods: kopf.Index,
    cluster_queues: kopf.Index,
    logger: kopf.Logger,
    **kwargs,
):
    custom = client.CustomObjectsApi(memo["api"])

    cluster = labels["dask.charmtx.com/cluster"]
    existing_workers = len(worker_pods.get((namespace, cluster), []))

    [(pod_queue, queue_lock)] = cluster_queues[namespace, cluster]
    async with queue_lock:
        pod_queue.discard(name)

    logger.info(f"Found {existing_workers} pods for {namespace}/{cluster} ({name})")
    # There is technically a race condition here, as both the cluster handler
    # and pod handler write to this condition. But both update the field with
    # the length of `worker_pods`, so it will eventually stabilize.
    await custom.patch_namespaced_custom_object_status(
        "dask.charmtx.com",
        "v1alpha1",
        namespace,
        "clusters",
        cluster,
        {"status": {"workers": {"count": existing_workers}}},
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
    cluster_queues: kopf.Index,
    logger: kopf.Logger,
    **kwargs,
):
    v1 = client.CoreV1Api(memo["api"])

    labels = {
        "dask.charmtx.com/cluster": name,
        "dask.charmtx.com/role": "worker",
    }
    if not status.get("scheduler"):
        raise kopf.TemporaryError(
            f"Scheduler not created yet for {namespace}/{name}", delay=1
        )

    [(pod_queue, queue_lock)] = cluster_queues[namespace, name]
    if pod_queue:
        raise kopf.TemporaryError(
            f"Created pods not yet processed: {', '.join(pod_queue)}", delay=1
        )

    existing_workers = status.get("workers", {}).get("count", 0)
    logger.info(f"Found {existing_workers} workers")
    required_workers = max(spec["worker"]["replicas"] - existing_workers, 0)
    logger.info(f"Creating {required_workers} workers")

    worker_metadata = {"generateName": f"{name}-worker-", "labels": labels}
    worker_template = templates.worker_template(
        spec["worker"]["template"], worker_metadata, status["scheduler"]["address"]
    )
    kopf.adopt(worker_template)

    async with queue_lock:
        pods = await asyncio.gather(
            *(
                v1.create_namespaced_pod(namespace, worker_template)
                for _ in range(required_workers)
            )
        )
        pod_names = {p.metadata.name for p in pods}
        pod_queue.update(pod_names)
        logger.info(f"Created pods: {', '.join(pod_names)}")
