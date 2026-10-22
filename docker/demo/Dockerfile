# The ticket-desk demo as an image: `demo serve`, `demo seed`
# and `demo bench` in one binary, built from the locked workspace.
#
#   docker build -f docker/demo/Dockerfile -t streamgres-demo .
ARG BASE_REGISTRY=docker.io/library
FROM ${BASE_REGISTRY}/rust:1.89-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock clippy.toml ./
COPY src ./src
COPY crates ./crates
RUN cargo build --release --locked -p streamgres-demo

FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10002 --home-dir /app --shell /usr/sbin/nologin demo \
    && mkdir -p /app && chown demo:demo /app
WORKDIR /app
COPY --from=builder /build/target/release/demo /app/demo
USER demo
EXPOSE 8080
ENTRYPOINT ["/app/demo"]
CMD ["serve"]
