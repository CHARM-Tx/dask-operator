import asyncio
from typing import NamedTuple, Optional
import aiohttp

import kopf
from kubernetes_asyncio import client, watch
import yarl


class Cluster(NamedTuple):
    name: str
    namespace: str
    port: int = 8786

    @classmethod
    def from_metadata(cls, metadata):
        return cls(name=metadata["name"], namespace=metadata["namespace"])

    @property
    def scheduler_url(self) -> yarl.URL:
        host = f"{self.name}-scheduler.{self.namespace}.svc"
        return yarl.URL.build(host=host, port=self.port)


class Autoscaler:
    def __init__(
        self, http: aiohttp.ClientSession, api: client.ApiClient, cluster: Cluster
    ):
        self.http = http
        self.api = api
        self.cluster = cluster

    async def get_desired_workers(self):
        url = self.cluster.scheduler_url.with_path("/api/v1/adaptive_target")
        return (await (await self.http.get(url)).json())["workers"]

    async def get_workers(self):
        url = self.cluster.scheduler_url.with_path("/api/v1/get_workers")
        return (await (await self.http.get(url)).json())["workers"]

    async def retire_workers(self, n: int):
        url = self.cluster.scheduler_url.with_path("/api/v1/retire_workers")
        if n == 0:
            return

        # Takes `n` with a number of workers, or `workers` with a list of worker
        params = {"n": 0}
        workers = await (await self.http.post(url, json=params)).json()
        return [worker["name"] for worker in workers]

    async def _wait_for_pods(self, labels: dict[str, str], n: int = 1):
        v1 = client.CoreV1Api(self.api)
        w = watch.Watch()
        label_selector = ",".join("=".join([k, v]) for k, v in labels.items())

        objects = set()
        async for event in w.stream(
            v1.list_namespaced_pod,
            namespace=self.cluster.namespace,
            label_selector=label_selector,
        ):
            conditions = event["object"].status.conditions or []
            if any(c.status == "True" for c in conditions if c.type == "Ready"):
                objects.add(event["object"].metadata.uid)

            if len(objects) >= n:
                return

    async def wait_for_workers(self, n: int):
        await self._wait_for_pods(
            {
                "dask.charmtx.com/cluster": self.cluster.name,
                "dask.charmtx.com/role": "worker",
            }
        )
        while len(await self.get_workers()) < n:
            asyncio.sleep(1)

    async def wait_ready(self):
        await self._wait_for_pods(
            {
                "dask.charmtx.com/cluster": self.cluster.name,
                "dask.charmtx.com/role": "scheduler",
            }
        )

        # Sometimes the service is not quite ready to serve despite the
        # scheduler being ready, so double check and wait.
        while True:
            try:
                await self.http.get(self.cluster.scheduler_url.with_path("/health"))
            except (aiohttp.ClientResponseError, aiohttp.ServerConnectionError):
                asyncio.sleep(1)
            else:
                break


@kopf.daemon("dask.charmtx.com", "clusters", cancellation_timeout=1.0)
async def autoscaler(
    name: str, namespace: str, memo: kopf.Memo, stopped: kopf.DaemonStopped, **kwargs
):
    v1 = client.CoreV1Api(memo["api"])
    scheduler = Cluster(name=name, namespace=namespace)

    async with aiohttp.ClientSession(raise_for_status=True) as http:
        autoscaler = Autoscaler(http, memo["api"], scheduler)
        while not stopped:
            await stopped.wait(5)  # Rate limiting
