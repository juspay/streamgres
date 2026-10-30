# The sync server as an image: built from the locked dependencies, and run on
# Google's distroless runtime, which has no shell, no package manager and no
# coreutils, so a compromised server has no tools to run. The runtime is
# `cc`: glibc and libgcc, which the binary links, and nothing else it needs;
# TLS is rustls with its roots compiled in. It runs as `nonroot` (uid 65532).
# Configured entirely by environment variables (docs/deploy.md lists every
# one); nothing is baked in but the listen address and the JSON log format a
# container platform expects.
#
# The health check is the binary itself (`server --health`), since there is
# no shell or curl to run one. Kubernetes ignores it and probes `/healthz`
# and `/readyz` over HTTP instead (docs/deploy.md).
#
#   docker build -f docker/server/Dockerfile -t streamgres .
#
# Both stages are on Debian 13, so the binary is linked against the glibc it
# runs on. A mirror of the same images is given whole:
#
#   docker build --build-arg BUILD_IMAGE=<mirror>/rust:1.89-trixie \
#     --build-arg RUNTIME_IMAGE=<mirror>/distroless/cc-debian13:nonroot ...
ARG BUILD_IMAGE=docker.io/library/rust:1.89-trixie
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot

FROM ${BUILD_IMAGE} AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock clippy.toml ./
COPY src ./src
RUN cargo build --release --locked --bin server \
    && mkdir -p /out/app /out/var/lib/streamgres \
    && cp target/release/server /out/app/server

FROM ${RUNTIME_IMAGE} AS runtime
# The binary is root-owned and read-only to the server; only the plan
# file's directory is its to write.
COPY --from=builder --chown=0:0 /out/app /app
COPY --from=builder --chown=65532:65532 /out/var/lib/streamgres /var/lib/streamgres
WORKDIR /app

ARG SOURCE_COMMIT=unknown
ENV SOURCE_COMMIT=${SOURCE_COMMIT} \
    STREAMGRES_ADDR=0.0.0.0:4848 \
    STREAMGRES_LOG_FORMAT=json

USER 65532:65532
EXPOSE 4848
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
    CMD ["/app/server", "--health"]
CMD ["/app/server"]
