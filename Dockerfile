ARG K8S_CODEGEN_VERSION=0.27.4

FROM golang:1-alpine AS build
ARG K8S_CODEGEN_VERSION

WORKDIR /app
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build  \
	go install k8s.io/code-generator/cmd/client-gen@v${K8S_CODEGEN_VERSION} \
	&& go install k8s.io/code-generator/cmd/deepcopy-gen@v${K8S_CODEGEN_VERSION} \
	&& go install k8s.io/code-generator/cmd/lister-gen@v${K8S_CODEGEN_VERSION} \
	&& go install k8s.io/code-generator/cmd/informer-gen@v${K8S_CODEGEN_VERSION} \
	&& go install k8s.io/code-generator/cmd/applyconfiguration-gen@v${K8S_CODEGEN_VERSION}

RUN --mount=type=cache,target=/root/.cache/go-build  \
	go generate ./...

RUN --mount=type=cache,target=/go --mount=type=cache,target=/root/.cache/go-build  \
	go build -o /out/dask-operator

FROM alpine

COPY --from=build /out/dask-operator /
ENTRYPOINT ["/dask-operator"]
