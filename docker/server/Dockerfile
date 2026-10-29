# The sync server as an image: built from the locked dependencies on Docker
# Hardened Images, and run on a hardened Debian runtime that has no shell,
# no package manager and no user but the unprivileged `nonroot` (uid 65532).
# Configured entirely by environment variables (docs/deploy.md lists every
# one); nothing is baked in but the listen address and the JSON log format a
# container platform expects.
#
# The runtime needs nothing installed: TLS roots are compiled into the binary
# (rustls with webpki-roots). There is no HEALTHCHECK: the platform probes
# `/healthz` and `/readyz` over HTTP (docs/deploy.md). Pulling from dhi.io
# needs `docker login dhi.io` first.
#
#   docker build -f docker/server/Dockerfile -t streamgres .
#
# A mirror of the same images is given whole, not as a registry prefix:
#
#   docker build --build-arg BUILD_IMAGE=<mirror>/rust:<tag> \
#     --build-arg RUNTIME_IMAGE=<mirror>/debian-base:<tag> ...
ARG BUILD_IMAGE=dhi.io/rust:1.89-debian13-dev
ARG RUNTIME_IMAGE=dhi.io/debian-base:debian13

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
CMD ["/app/server"]
