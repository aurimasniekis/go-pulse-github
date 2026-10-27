# syntax=docker/dockerfile:1.7
#
# pulse-github container image.
#
#   build : static Go binary (CGO_ENABLED=0, pure-Go SQLite), cross-compiled on the build platform (no emulation).
#   final : distroless static (Debian 13), non-root UID/GID 65532, no shell, CA certificates included.
#
# Base images are pinned by digest (the same digests go-pulse v0.1.3 uses). Update a digest with:
#   docker buildx imagetools inspect <image>:<tag>
#
# Runtime layout:
#   /usr/local/bin/pulse-github     binary (root-owned, read-only)
#   /data                           state volume (SQLite database, WAL, lock file), 0700 65532:65532
#   /etc/pulse-github/config.yaml   configuration (mounted read-only)
#   /run/secrets/*                  credentials (mounted read-only; never baked into the image)

ARG GO_IMAGE=docker.io/library/golang:1.27.3-trixie@sha256:fbb7ae40b62ff8afe31662df570d39b4f5052ac32b90dae8e60d7657a20a74f4
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=
ARG BUILD_DATE=
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-mod=readonly
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download && go mod verify
COPY cmd/ cmd/
COPY internal/ internal/
COPY migrations/ migrations/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -buildvcs=false \
      -ldflags "-s -w -buildid= \
        -X github.com/aurimasniekis/go-pulse-github/internal/buildinfo.Version=${VERSION} \
        -X github.com/aurimasniekis/go-pulse-github/internal/buildinfo.Commit=${COMMIT} \
        -X github.com/aurimasniekis/go-pulse-github/internal/buildinfo.Date=${BUILD_DATE}" \
      -o /out/pulse-github ./cmd/pulse-github
# Directory skeleton (the final image has no shell). Named volumes inherit ownership and mode on first use.
RUN install -d -m 0700 -o 65532 -g 65532 /rootfs/data && install -d -m 0755 /rootfs/etc/pulse-github

FROM ${RUNTIME_IMAGE}
ARG VERSION=dev
ARG COMMIT=
LABEL org.opencontainers.image.title="pulse-github" \
      org.opencontainers.image.description="Collects GitHub activity into Pulse (NATS JetStream)" \
      org.opencontainers.image.source="https://github.com/aurimasniekis/go-pulse-github" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.base.name="gcr.io/distroless/static-debian13:nonroot"
COPY --from=build --chown=0:0 --chmod=0555 /out/pulse-github /usr/local/bin/pulse-github
COPY --from=build /rootfs/ /
ENV PULSE_GITHUB_CONFIG=/etc/pulse-github/config.yaml PULSE_GITHUB_STORAGE_PATH=/data/pulse-github.db
USER 65532:65532
WORKDIR /data
VOLUME ["/data"]
# 8080 webhook receiver (behind a TLS reverse proxy), 9090 health/readiness/metrics (keep private).
EXPOSE 8080/tcp 9090/tcp
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 CMD ["/usr/local/bin/pulse-github", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/pulse-github"]
CMD ["run"]
