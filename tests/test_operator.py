import pytest
from kubernetes_asyncio import client, watch
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
                    "service": {"type": "NodePort"},
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


@pytest.mark.parametrize(
    ["type", "role", "expected_count"],
    [
        ("service", "scheduler", 1),
        ("pod", "scheduler", 1),
        ("pod", "worker", 2),
    ],
)
async def test_create_resources(
    type, role, expected_count, dask_cluster, api: ApiClient
):
    v1 = client.CoreV1Api(api)
    list_fn = getattr(v1, f"list_namespaced_{type}")
    w = watch.Watch()

    cluster_name = dask_cluster["metadata"]["name"]
    label_selector = ",".join(
        [f"dask.charmtx.com/role={role}", f"dask.charmtx.com/cluster={cluster_name}"]
    )

    objects = set()
    async for event in w.stream(
        list_fn,
        namespace=dask_cluster["metadata"]["namespace"],
        label_selector=label_selector,
        timeout_seconds=1,
    ):
        objects.add(event["object"].metadata.uid)
        if len(objects) == expected_count:
            break
    else:
        pytest.fail(
            f"Resources not ready, expected {expected_count} got {len(objects)}."
        )
