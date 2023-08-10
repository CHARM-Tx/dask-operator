import os

import kopf

from kubernetes_asyncio.config import (
    load_kube_config,
    load_incluster_config,
    ConfigException,
)
from kubernetes_asyncio.client import Configuration, ApiClient
from kubernetes_asyncio import client


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
async def create_cluster(spec, namespace, memo, logger, **kwargs):
    v1 = client.CoreV1Api(memo["api"])
    adoptee = {}
    kopf.adopt(adoptee)

    pod = client.V1Pod(
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="ubuntu",
                    image="ubuntu:22.04",
                    command=["bash", "-c"],
                    args=["trap : TERM INT; sleep infinity & wait"],
                )
            ]
        ),
        **adoptee
    )
    await v1.create_namespaced_pod(namespace=namespace, body=pod)
