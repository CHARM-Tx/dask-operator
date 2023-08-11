from typing import Any, Union

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


def _get_merge_key(item: Union[object, dict[str, Any]], merge_key: str):
    if hasattr(item, "openapi_types"):
        return getattr(item, merge_key)
    else:
        return item[merge_key]


def get(
    items: list[object | dict[str, Any]], key: str, merge_key: str = "name"
) -> dict[str, Any]:
    [item] = [i for i in items if key == _get_merge_key(i, merge_key)]
    return item


http_port = 8787
probes = {
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

scheduler_container = {"name": "scheduler", "command": ["dask", "scheduler"], **probes}

scheduler_ports = {"tcp-comm": 8786, "http-dashboard": http_port}
scheduler_env = [
    # The scheduler API is disabled by default, see https://github.com/dask/distributed/issues/6407
    {
        "name": "DASK_DISTRIBUTED__SCHEDULER__HTTP__ROUTES",
        "value": "['distributed.http.scheduler.api','distributed.http.health']",
    }
]

scheduler_template_ports = [
    {"name": name, "containerPort": port, "protocol": "TCP"}
    for name, port in scheduler_ports.items()
]

scheduler_service_ports = [
    {"name": name, "port": port, "targetPort": name, "protocol": "TCP"}
    for name, port in scheduler_ports.items()
]


worker_container = {
    "name": "worker",
    "command": [
        "dask",
        "worker",
        "--name=$(DASK_WORKER_NAME)",
        "--no-nanny",
        f"--dashboard-address=$(DASK_WORKER_NAME):{http_port}",
    ],
    **probes,
}

worker_env = [
    {
        "name": "DASK_WORKER_NAME",
        "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
    }
]

worker_ports = [
    {"name": "http-dashboard", "containerPort": http_port, "protocol": "TCP"}
]


def scheduler_template(template, labels: dict[str, str]) -> list[dict[str, Any]]:
    metadata = template.get("metadata", {})
    labels = metadata.get("labels", {}) | labels

    containers = {c["name"]: c for c in template["spec"]["containers"]}
    containers["scheduler"] = scheduler_container | containers["scheduler"]
    containers["scheduler"]["env"] = merge(
        containers["scheduler"].get("env", []), [*scheduler_env]
    )
    containers["scheduler"]["ports"] = merge(
        containers["scheduler"].get("ports", []),
        scheduler_template_ports,
    )

    return {
        "metadata": {**metadata, "labels": labels},
        "spec": {**template["spec"], "containers": list(containers.values())},
    }


def scheduler_service(spec, labels: dict[str, str]) -> list[dict[str, Any]]:
    spec = spec | {"ports": merge(scheduler_service_ports, spec.get("ports", {}))}
    return client.V1ServiceSpec(**spec, selector=spec.get("selector", {}) | labels)


def worker_template(
    template, labels: dict[str, str], scheduler: Any
) -> list[dict[str, Any]]:
    metadata = template.get("metadata", {})
    labels = metadata.get("labels", {}) | labels

    scheduler_name = scheduler["metadata"]["name"]
    scheduler_namespace = scheduler["metadata"]["namespace"]
    scheduler_port = get(scheduler["spec"].ports, "tcp-comm")["port"]
    scheduler_address = (
        f"tcp://{scheduler_name}.{scheduler_namespace}.svc:{scheduler_port}"
    )

    containers = {c["name"]: c for c in template["spec"]["containers"]}
    containers["worker"] = worker_container | containers["worker"]
    containers["worker"]["env"] = merge(
        containers["worker"].get("env", []),
        [*worker_env, {"name": "DASK_SCHEDULER_ADDRESS", "value": scheduler_address}],
    )
    containers["worker"]["ports"] = merge(
        containers["worker"].get("ports", []), worker_ports
    )

    return {
        "metadata": {**metadata, "labels": labels},
        "spec": {**template["spec"], "containers": list(containers.values())},
    }
