# FreeBSD CSI Driver - Container Image
# Multi-stage build for minimal runtime image

# Stage 1: Build
FROM rust:1.82-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY csi-driver/ csi-driver/
COPY ctld-agent/ ctld-agent/

# Build only csi-driver (ctld-agent runs natively on FreeBSD, not in container)
RUN cargo build -p csi-driver --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Version and metadata labels
ARG VERSION=dev
LABEL org.opencontainers.image.title="FreeBSD CSI Driver"
LABEL org.opencontainers.image.description="Kubernetes CSI driver for FreeBSD ZFS storage"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.source="https://github.com/ndenev/freebsd-csi"
LABEL org.opencontainers.image.licenses="BSD-2-Clause"

# Install runtime dependencies for iSCSI and NVMeoF operations
RUN apt-get update && apt-get install -y --no-install-recommends \
    # iSCSI initiator tools
    open-iscsi \
    # NVMe-oF tools
    nvme-cli \
    # Filesystem tools for formatting volumes
    e2fsprogs \
    xfsprogs \
    # Mount utilities
    mount \
    # TLS support
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    # Create directories for CSI socket
    && mkdir -p /var/run/csi /csi

# Copy the binary from builder stage
COPY --from=builder /build/target/release/csi-driver /usr/local/bin/csi-driver

# CSI driver typically needs root for mount operations
# Running as non-root would require additional capabilities

ENTRYPOINT ["/usr/local/bin/csi-driver"]
