FROM golang:1-bookworm AS crd

RUN --mount=type=cache,target=/go --mount=type=cache,target=/root/.cache/go-build  \
    go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.12.1

WORKDIR /app
COPY ./src/api ./
RUN --mount=type=cache,target=/go --mount=type=cache,target=/root/.cache/go-build \
    go build ./... && \
    controller-gen paths=./... crd:maxDescLen=0

FROM python:3.11 AS base

WORKDIR /app

FROM base AS dev

COPY requirements-dev.txt pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-dev.txt
COPY . .
RUN pip install --no-deps --editable .[dev]
COPY --from=crd --link /app/config/crd ./src/dask_operator/crd

FROM base

COPY requirements.txt pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
COPY . .
RUN pip install --no-deps .
