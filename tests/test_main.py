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
    crd_file = Path(dask_operator.__file__).parent / "daskcluster.yaml"
    crd = yaml.safe_load(crd_file.read_text())

    apiextensions = client.ApiextensionsV1Api(api)
    r = await apiextensions.create_custom_resource_definition(crd)
    yield r
    await apiextensions.delete_custom_resource_definition(r.metadata.name)


@pytest.fixture(scope="session")
def operator(kubeconfig):
    with pytest.MonkeyPatch.context() as ctx:
        ctx.setenv("KUBECONFIG", str(kubeconfig.absolute()))
        with KopfRunner(["run", "--all-namespaces", "-m", "dask_operator"]) as runner:
            time.sleep(0.01)  # Otherwise, there is an error on teardown
            yield runner
    assert runner.exit_code == 0


async def test_create(crd, operator, api: ApiClient):
    custom = client.CustomObjectsApi(api)
    r = await custom.create_namespaced_custom_object(
        "charmtx.com",
        "v1alpha1",
        "default",
        "daskclusters",
        {
            "apiVersion": "charmtx.com/v1alpha1",
            "kind": "DaskCluster",
            "metadata": {"generateName": "test-"},
            "spec": {"image": "foo", "replicas": 1},
        },
    )
    v1 = client.CoreV1Api(api)
    w = watch.Watch()
    async for event in w.stream(
        v1.list_namespaced_pod, namespace=r["metadata"]["namespace"], timeout_seconds=10
    ):
        assert event["object"].metadata.name.startswith("test-")
        w.stop()

    await custom.delete_namespaced_custom_object(
        "charmtx.com",
        "v1alpha1",
        r["metadata"]["namespace"],
        "daskclusters",
        r["metadata"]["name"],
    )
