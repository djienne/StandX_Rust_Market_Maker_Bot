FROM rust:1.92-bookworm AS build
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY config.json config.test.json ./
RUN CARGO_HTTP_MULTIPLEXING=false cargo build --release --locked --bin standx-orderbook

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/standx-orderbook /usr/local/bin/standx-orderbook
COPY config.json config.test.json /app/
WORKDIR /data
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/standx-orderbook"]
CMD ["/app/config.json"]
