# The ticket-desk example as an image: `example serve`, `example seed`
# and `example bench` in one binary, built from the locked workspace.
#
#   docker build -f docker/example/Dockerfile -t streamgres-example .
ARG BASE_REGISTRY=docker.io/library
FROM ${BASE_REGISTRY}/rust:1.89-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock clippy.toml ./
COPY src ./src
COPY crates ./crates
RUN cargo build --release --locked -p streamgres_example

FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10002 --home-dir /app --shell /usr/sbin/nologin example \
    && mkdir -p /app && chown example:example /app
WORKDIR /app
COPY --from=builder /build/target/release/example /app/example
USER example
EXPOSE 8080
ENTRYPOINT ["/app/example"]
CMD ["serve"]
