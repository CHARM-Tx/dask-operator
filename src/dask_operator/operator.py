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
    appsv1 = client.AppsV1Api(memo["api"])
    labels = {
        "dask.charmtx.com/cluster-name": name,
    }

    scheduler_labels = {**labels, "dask.charmtx.com/role": "scheduler"}
    scheduler_template = spec["scheduler"]["template"]
    scheduler_template["spec"]["containers"] = templates.scheduler_containers(
        scheduler_template["spec"]["containers"]
    )
    scheduler_template.setdefault("metadata", {}).setdefault("labels", {})
    scheduler_template["metadata"]["labels"] |= scheduler_labels

    scheduler = {
        "metadata": {"generateName": f"{name}-", "labels": scheduler_labels},
        "spec": client.V1ReplicaSetSpec(
            replicas=1,
            selector={"matchLabels": scheduler_labels},
            template=scheduler_template,
        ),
    }

    kopf.adopt(scheduler)
    await appsv1.create_namespaced_replica_set(namespace=namespace, body=scheduler)
