# syntax=docker/dockerfile:1.7

FROM --platform=$TARGETPLATFORM golang:1.27-trixie AS build

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

# The TigerBeetle client is linked with cgo, so the runtime needs glibc. The distroless cc image
# provides glibc and CA certificates, no shell or package manager, and runs as uid 65532.
FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

COPY --from=build /out/tb-cdc-nats /usr/local/bin/tb-cdc-nats

ENTRYPOINT ["/usr/local/bin/tb-cdc-nats"]
