from typing import NamedTuple
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


class Scheduler:
    def __init__(
        self, http: aiohttp.ClientSession, api: client.ApiClient, cluster: Cluster
    ):
        self.http = http
        self.api = api
        self.cluster = cluster

    async def is_ready(self):
        url = self.cluster.scheduler_url.with_path("/health")
        return await self.http.get(url)

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
        # names. Note: number of workers actually retired may be <= n.
        params = {"n": n}
        workers = await (await self.http.post(url, json=params)).json()
        return [worker["id"] for worker in workers.values()]

    async def _wait_for_pods(self, labels: dict[str, str], n: int = 1):
        v1 = client.CoreV1Api(self.api)
        w = watch.Watch()
        labels = {"dask.charmtx.com/cluster": self.cluster.name, **labels}
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

    async def wait_ready(self):
        await self._wait_for_pods({"dask.charmtx.com/role": "scheduler"})


@kopf.on.startup()
async def http(memo: kopf.Memo, **kwargs):
    memo["http"] = aiohttp.ClientSession(raise_for_status=True)


@kopf.on.cleanup()
async def http_close(memo: kopf.Memo, **kwargs):
    await memo["http"].close()


def clamp(i, lower, upper):
    return min(max(i, lower), upper)


@kopf.timer("dask.charmtx.com", "clusters", interval=5.0, idle=5.0)
async def update_autoscaler(
    name: str,
    namespace: str,
    memo: kopf.Memo,
    spec: kopf.Spec,
    **kwargs,
):
    custom = client.CustomObjectsApi(memo["api"])
    scheduler = Cluster(name=name, namespace=namespace)
    scheduler = Scheduler(memo["http"], memo["api"], scheduler)
    try:
        await scheduler.is_ready()
    except aiohttp.ClientError:
        await scheduler.wait_ready()
        return kopf.TemporaryError(delay=1.0)

    desired = await scheduler.get_desired_workers()
    desired = clamp(
        desired, spec["worker"]["maxReplicas"], spec["worker"]["minReplicas"]
    )

    await custom.patch_namespaced_custom_object(
        "dask.charmtx.com",
        "v1alpha1",
        namespace,
        "clusters",
        name,
        {"spec": {"worker": {"replicas": desired}}},
    )
    # Race condition: must patch replicas _before_ retiring workers, so the
    # retired workers are not recreated if they exit quickly.
    await scheduler.retire_workers(max(spec["worker"]["replicas"] - desired, 0))
