from copy import deepcopy
import operator as op

from kubernetes_asyncio.client import models
from dask_operator import templates
import pytest


@pytest.mark.parametrize("cls", [models.V1EnvVar, dict])
def test_get(cls):
    objs = [cls(name="foo", value="bar"), cls(name="baz", value="qux")]

    assert templates.get(objs, "foo") == cls(name="foo", value="bar")


def test_merge():
    base = [{"name": "x", "foo": "bar"}, {"name": "y", "foo": "baz"}]
    base_ref = deepcopy(base)
    update = [{"name": "x", "foo": "bar-2"}, {"name": "z", "foo": "baz-2"}]
    update_ref = deepcopy(update)
    expected = [
        {"name": "y", "foo": "baz"},
        {"name": "x", "foo": "bar-2"},
        {"name": "z", "foo": "baz-2"},
    ]
    result = templates.merge(base, update)

    by_key = op.itemgetter("name")
    assert sorted(result, key=by_key) == sorted(expected, key=by_key)
    # Ensure no in-place changes
    assert base_ref == base
    assert update_ref == update


def test_scheduler_template():
    template = {
        "spec": {
            "containers": [{"name": "scheduler", "image": "ghcr.io/dask/dask:latest"}]
        }
    }
    labels = {"foo": "bar"}
    new_template = templates.scheduler_template(template, labels)

    by_key = op.itemgetter("name")
    assert sorted(new_template["spec"]["containers"][0]["ports"], key=by_key) == sorted(
        templates.scheduler_template_ports, key=by_key
    )

    assert new_template["metadata"]["labels"] == labels


def test_scheduler_service():
    spec = {}
    labels = {"foo": "bar"}
    new_spec = templates.scheduler_service(spec, labels)

    by_key = op.itemgetter("name")
    assert sorted(new_spec.ports, key=by_key) == sorted(
        templates.scheduler_service_ports, key=by_key
    )

    assert new_spec.selector == labels


def test_worker_template():
    scheduler = {
        "metadata": {"name": "foo", "namespace": "bar"},
        "spec": models.V1ServiceSpec(ports=[{"name": "tcp-comm", "port": 123}]),
    }
    template = {
        "spec": {
            "containers": [{"name": "worker", "image": "ghcr.io/dask/dask:latest"}]
        }
    }
    labels = {"foo": "bar"}
    new_template = templates.worker_template(template, labels, scheduler)

    assert (
        templates.get(
            new_template["spec"]["containers"][0]["env"], "DASK_SCHEDULER_ADDRESS"
        )["value"]
        == f"tcp://foo.bar.svc:123"
    )

    assert new_template["metadata"]["labels"] == labels
