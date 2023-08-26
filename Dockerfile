FROM golang:1-alpine AS build

WORKDIR /app
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build  \
    go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.12.1 \
	&& go install k8s.io/code-generator/cmd/client-gen@v0.27.4 \
	&& go install k8s.io/code-generator/cmd/deepcopy-gen@v0.27.4 \
	&& go install k8s.io/code-generator/cmd/lister-gen@v0.27.4 \
	&& go install k8s.io/code-generator/cmd/informer-gen@v0.27.4

RUN --mount=type=cache,target=/root/.cache/go-build  \
	go generate ./...

RUN --mount=type=cache,target=/go --mount=type=cache,target=/root/.cache/go-build  \
    go build -o /out/dask-operator

FROM alpine

COPY --from=build /out/dask-operator /
ENTRYPOINT ["/dask-operator"]
