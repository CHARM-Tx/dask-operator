import os

from kubernetes_asyncio.config import load_kube_config
from kubernetes_asyncio.client import ApiClient, Configuration
from kubernetes_asyncio import client


async def main():
    config = Configuration()
    await load_kube_config(os.environ.get("KUBECONFIG"), client_configuration=config)

    async with ApiClient(configuration=config) as api:
        v1 = client.CoreV1Api(api)
        for i in (await v1.list_pod_for_all_namespaces()).items:
            ...
