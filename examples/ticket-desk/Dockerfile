FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --manifest-path examples/ticket-desk/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home demo
COPY --from=build /src/target/release/demo /usr/local/bin/demo
USER demo
EXPOSE 8080
ENTRYPOINT ["demo"]
CMD ["serve", "--init", "--listen", "0.0.0.0:8080"]
