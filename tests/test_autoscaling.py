import asyncio
import pytest
import aiohttp
from yarl import URL

from dask_operator import autoscaling
from kubernetes_asyncio import client, watch
from kubernetes_asyncio.client import ApiClient


async def wait_for_scheduler(autoscaler):
    await autoscaler._wait_for_pods({"dask.charmtx.com/role": "scheduler"})
    while True:
        try:
            await autoscaler.is_ready()
        except (
            aiohttp.ClientOSError,
            aiohttp.ClientResponseError,
            aiohttp.ServerConnectionError,
        ):
            await asyncio.sleep(0.1)
        else:
            break


async def wait_for_workers(autoscaler, n: int):
    await autoscaler._wait_for_pods({"dask.charmtx.com/role": "worker"}, n=n)
    while True:
        workers = await autoscaler.get_workers()
        if len(workers) < n:
            await asyncio.sleep(1)
        elif len(workers) == n:
            break
        else:
            raise ValueError(f"Got too many pods: {n}")


@pytest.fixture(scope="session")
async def dask_cluster(operator, api: ApiClient):
    custom = client.CustomObjectsApi(api)
    r = await custom.create_namespaced_custom_object(
        "dask.charmtx.com",
        "v1alpha1",
        "default",
        "clusters",
        {
            "apiVersion": "dask.charmtx.com/v1alpha1",
            "kind": "Cluster",
            "metadata": {"generateName": "test-"},
            "spec": {
                "scheduler": {
                    "service": {
                        # Must be node-port, so pytest process can reach the scheduler
                        "type": "NodePort",
                        "ports": [
                            {
                                "name": "http-dashboard",
                                "nodePort": 30000,
                                "port": 8787,
                                "targetPort": "http-dashboard",
                            }
                        ],
                    },
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "scheduler",
                                    "image": "ghcr.io/dask/dask:latest",
                                }
                            ]
                        }
                    },
                },
                "worker": {
                    "replicas": 2,
                    "minReplicas": 1,
                    "maxReplicas": 2,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "worker",
                                    "image": "ghcr.io/dask/dask:latest",
                                }
                            ]
                        }
                    },
                },
            },
        },
    )

    yield r

    await custom.delete_namespaced_custom_object(
        "dask.charmtx.com",
        "v1alpha1",
        r["metadata"]["namespace"],
        "clusters",
        r["metadata"]["name"],
    )


@pytest.fixture(scope="session")
async def http():
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        yield session


@pytest.fixture()
async def autoscaler(http, dask_cluster, monkeypatch, api: ApiClient):
    monkeypatch.setattr(
        autoscaling.Cluster, "scheduler_url", URL("http://host.docker.internal:30000")
    )
    # Not the real service details, but this causes the URL to be correct
    scheduler = autoscaling.Cluster.from_metadata(dask_cluster["metadata"])
    autoscaler = autoscaling.Scheduler(http, api, scheduler)

    # Scheduler can take quite some time to come up after pods exist
    await asyncio.wait_for(wait_for_scheduler(autoscaler), timeout=30)
    await asyncio.wait_for(wait_for_workers(autoscaler, n=2), timeout=30)
    yield autoscaler


async def test_get_desired_workers(autoscaler):
    assert (await autoscaler.get_desired_workers()) == 0


async def test_get_workers(autoscaler):
    assert len(await autoscaler.get_workers()) == 2


async def test_retire_workers(dask_cluster, autoscaler, api):
    v1 = client.CoreV1Api(api)
    w = watch.Watch()
    worker_labels = {
        "dask.charmtx.com/cluster": dask_cluster["metadata"]["name"],
        "dask.charmtx.com/role": "worker",
    }
    label_selector = ",".join(map("=".join, worker_labels.items()))
    stream = w.stream(
        v1.list_namespaced_pod,
        namespace=dask_cluster["metadata"]["namespace"],
        label_selector=label_selector,
        timeout_seconds=30,
    )

    retired = set(await autoscaler.retire_workers(1))
    assert len(retired) == 1
    async for event in stream:
        pod_name = event["object"].metadata.name
        if event["type"] == "DELETED" and pod_name in retired:
            retired.remove(pod_name)
        if len(retired) == 0:
            break
    else:
        pytest.fail(f"Pods not deleted: {retired}")
