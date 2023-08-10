import asyncio
from pathlib import Path
import time
import pytest
import yaml
from yarl import URL
from kopf.testing import KopfRunner
from kubernetes_asyncio import client, watch
from kubernetes_asyncio.client import Configuration, ApiClient
from kubernetes_asyncio.config import load_kube_config

import dask_operator
import dask_operator.operator


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def kubeconfig(tmp_path_factory):
    path = Path(__file__).parent / ".." / "kubeconfig"
    contents = yaml.safe_load(path.open())
    [cluster] = contents["clusters"]
    server = URL(cluster["cluster"]["server"])
    cluster["cluster"]["server"] = str(server.with_host("host.docker.internal"))
    out_path = tmp_path_factory.mktemp("kube") / "config"
    with out_path.open("w") as f:
        yaml.dump(contents, f)
        yield out_path


@pytest.fixture(scope="session")
async def api(kubeconfig):
    config = Configuration()
    await load_kube_config(str(kubeconfig), client_configuration=config)
    async with ApiClient(configuration=config) as api:
        yield api


@pytest.fixture(scope="session")
async def crd(api: ApiClient):
    apiextensions = client.ApiextensionsV1Api(api)

    crd_dir = Path(dask_operator.__file__).parent / "crd"
    [crd_file] = crd_dir.iterdir()
    crd = yaml.safe_load(crd_file.read_text())
    r = await apiextensions.create_custom_resource_definition(crd)

    # Need to wait for the CRDs to actually be created/registered
    w = watch.Watch()
    async for event in w.stream(
        apiextensions.list_custom_resource_definition, timeout_seconds=10
    ):
        assert event["object"].metadata.name == r.metadata.name
        if any(
            c.type == "Established" and c.status == "True"
            for c in event["object"].status.conditions or []
        ):
            w.stop()

    yield r
    await apiextensions.delete_custom_resource_definition(r.metadata.name)


@pytest.fixture(scope="session")
async def operator(kubeconfig, crd):
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("KUBECONFIG", str(kubeconfig.absolute()))
        with KopfRunner(["run", "--all-namespaces", "-m", "dask_operator"]) as runner:
            time.sleep(0.01)  # Otherwise, there is an error on teardown
            yield runner
    assert runner.exit_code == 0


async def test_create(operator, api: ApiClient):
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
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "scheduler",
                                    "image": "ghcr.io/dask/dask:latest",
                                }
                            ]
                        }
                    }
                }
            },
        },
    )

    v1 = client.CoreV1Api(api)
    w = watch.Watch()
    matching_pods = 0
    async for event in w.stream(
        v1.list_namespaced_pod,
        namespace=r["metadata"]["namespace"],
        label_selector="dask.charmtx.com/role=scheduler",
        timeout_seconds=10,
    ):
        assert event["object"].metadata.name.startswith("test-")
        matching_pods += 1
        w.stop()

    matching_services = 0
    async for event in w.stream(
        v1.list_namespaced_service,
        namespace=r["metadata"]["namespace"],
        label_selector="dask.charmtx.com/role=scheduler",
        timeout_seconds=10,
    ):
        assert event["object"].metadata.name.startswith("test-")
        matching_services += 1
        w.stop()

    await custom.delete_namespaced_custom_object(
        "dask.charmtx.com",
        "v1alpha1",
        r["metadata"]["namespace"],
        "clusters",
        r["metadata"]["name"],
    )

    assert matching_pods == 1
    assert matching_services == 1
