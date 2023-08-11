import asyncio
import pytest
import aiohttp
from yarl import URL

from dask_operator import autoscaling
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient


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
                    "minReplicas": 2,
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
    autoscaler = autoscaling.Autoscaler(http, api, scheduler)
    # Scheduler can take quite some time to come up
    await asyncio.wait_for(autoscaler.wait_ready(), timeout=30)
    await asyncio.wait_for(autoscaler.wait_for_workers(2), timeout=30)
    yield autoscaler


async def test_get_desired_workers(autoscaler):
    assert (await autoscaler.get_desired_workers()) == 0


async def test_get_workers(autoscaler):
    assert len(await autoscaler.get_workers()) == 2


async def test_retire_workers(autoscaler):
    retired = await autoscaler.retire_workers(1)
    # FIXME: There is no non-racy way to scale down workers with replicasets.
    # Retiring the worker causes the process to gracefully exit, at which point
    # the replicaset restarts it, and it re-registers as a new worker.
