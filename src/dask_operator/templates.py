from typing import Any


# This is a hacky imitation of k8s strategic-merge patches, which aren't
# supported for CRDs.
# TODO: Find a way to extract `merge_key` from the resource's API definition
def merge(
    base: list[dict[str, Any]], update: list[dict[str, Any]], merge_key: str = "name"
) -> list[dict[str, Any]]:
    by_key = {item[merge_key]: item for item in base}
    for item in update:
        key = item[merge_key]
        # TODO: Make this recursive
        by_key[key] = by_key.get(key, {}) | item
    return list(by_key.values())


def get(
    items: list[dict[str, Any]], key: str, merge_key: str = "name"
) -> dict[str, Any]:
    [item] = [i for i in items if i[merge_key] == key]
    return item


scheduler_container = {
    "name": "scheduler",
    "command": ["dask", "scheduler"],
    "readinessProbe": {
        "httpGet": {"port": "http-dashboard", "path": "/health"},
        "initialDelaySeconds": 5,
        "periodSeconds": 10,
    },
    "livenessProbe": {
        "httpGet": {"port": "http-dashboard", "path": "/health"},
        "initialDelaySeconds": 15,
        "periodSeconds": 20,
    },
}

scheduler_ports = [
    {"name": "tcp-comm", "containerPort": 8786, "protocol": "TCP"},
    {"name": "http-dashboard", "containerPort": 8787, "protocol": "TCP"},
]


def scheduler_containers(spec) -> list[dict[str, Any]]:
    containers = {c["name"]: c for c in spec}
    containers["scheduler"] = scheduler_container | containers["scheduler"]
    containers["scheduler"]["ports"] = merge(
        containers["scheduler"].get("ports", []), scheduler_ports
    )
    return list(containers.values())
