from copy import deepcopy
import operator as op

from dask_operator import templates


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
