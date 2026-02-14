# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.23-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.buildVersion=${VERSION}" \
    -o /out/tb-cdc-nats ./cmd/tb-cdc-nats

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/tb-cdc-nats /usr/local/bin/tb-cdc-nats

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/tb-cdc-nats"]
