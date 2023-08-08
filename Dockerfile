FROM python:3.11 AS base

WORKDIR /app

FROM base AS dev

COPY requirements-dev.txt pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-dev.txt
COPY . .
RUN pip install --no-deps --editable .[dev]

FROM base

COPY requirements.txt pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
COPY . .
RUN pip install --no-deps .
