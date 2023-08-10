from typing import Any

from kubernetes_asyncio import client


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

scheduler_ports = {"tcp-comm": 8786, "http-dashboard": 8787}

scheduler_template_ports = [
    {"name": name, "containerPort": port, "protocol": "TCP"}
    for name, port in scheduler_ports.items()
]

scheduler_service_ports = [
    {"name": name, "port": port, "targetPort": name, "protocol": "TCP"}
    for name, port in scheduler_ports.items()
]


def scheduler_template(template, labels) -> list[dict[str, Any]]:
    metadata = template.get("metadata", {})
    labels = metadata.get("labels", {}) | labels

    containers = {c["name"]: c for c in template["spec"]["containers"]}
    containers["scheduler"] = scheduler_container | containers["scheduler"]
    containers["scheduler"]["ports"] = merge(
        scheduler_template_ports, containers["scheduler"].get("ports", [])
    )

    return {
        "metadata": {**metadata, "labels": labels},
        "spec": {**template["spec"], "containers": list(containers.values())},
    }


def scheduler_service(spec, labels) -> list[dict[str, Any]]:
    spec = spec | {"ports": merge(scheduler_service_ports, spec.get("ports", {}))}
    return client.V1ServiceSpec(**spec, selector=spec.get("selector", {}) | labels)
