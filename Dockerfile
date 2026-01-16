# Stage 1: Builder
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/standx-orderbook .

# Create data directory for wallet_history.csv
RUN mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser

# Default command - config path can be overridden
CMD ["./standx-orderbook", "/app/config.json"]
