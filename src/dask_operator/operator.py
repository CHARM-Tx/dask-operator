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
async def create_cluster(
    name: str, namespace: str, spec: kopf.Spec, memo: kopf.Memo, **kwargs
):
    v1 = client.CoreV1Api(memo["api"])
    appsv1 = client.AppsV1Api(memo["api"])
    labels = {"dask.charmtx.com/cluster": name}

    scheduler_labels = {**labels, "dask.charmtx.com/role": "scheduler"}
    scheduler_replicaset = {
        "metadata": {"name": f"{name}-scheduler", "labels": scheduler_labels},
        "spec": client.V1ReplicaSetSpec(
            replicas=1,
            selector={"matchLabels": scheduler_labels},
            template=templates.scheduler_template(
                spec["scheduler"]["template"], scheduler_labels
            ),
        ),
    }
    kopf.adopt(scheduler_replicaset)

    scheduler_service = {
        "metadata": {"name": f"{name}-scheduler", "labels": scheduler_labels},
        "spec": templates.scheduler_service(
            spec["scheduler"].get("service", {}), scheduler_labels
        ),
    }
    kopf.adopt(scheduler_service)

    worker_labels = {**labels, "dask.charmtx.com/role": "worker"}
    worker_replicaset = {
        "metadata": {"name": f"{name}-worker", "labels": worker_labels},
        "spec": client.V1ReplicaSetSpec(
            replicas=spec["worker"].get("replicas", 0),
            selector={"matchLabels": worker_labels},
            template=templates.worker_template(
                spec["worker"]["template"], worker_labels, scheduler_service
            ),
        ),
    }
    kopf.adopt(worker_replicaset)

    await asyncio.gather(
        appsv1.create_namespaced_replica_set(
            namespace=namespace, body=scheduler_replicaset
        ),
        appsv1.create_namespaced_replica_set(
            namespace=namespace, body=worker_replicaset
        ),
        v1.create_namespaced_service(namespace=namespace, body=scheduler_service),
    )
