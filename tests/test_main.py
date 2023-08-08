from pathlib import Path
import pytest
import yaml
from yarl import URL

from dask_operator.__main__ import main


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


def test_main(monkeypatch, event_loop, kubeconfig):
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig.absolute()))
    event_loop.run_until_complete(main())
