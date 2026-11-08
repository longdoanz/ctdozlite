# Build context = repo root:  docker build -f deploy/ctdoz-daemon/Dockerfile -t ctdoz-daemon .
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      cmake pkg-config libasound2-dev libxkbcommon-dev libxcb-render0-dev \
      libxcb-shape0-dev libxcb-xfixes0-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
ENV CMAKE_POLICY_VERSION_MINIMUM=3.5
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin ctdoz-daemon && \
    cp target/release/ctdoz-daemon /usr/local/bin/ctdoz-daemon

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libasound2 tini && rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 ctdoz && useradd --system --uid 10001 --gid 10001 --home /data ctdoz && install -d -o ctdoz -m700 /data
COPY --from=build /usr/local/bin/ctdoz-daemon /usr/local/bin/ctdoz-daemon
COPY deploy/ctdoz-daemon/entrypoint.sh /usr/local/bin/entrypoint.sh
USER ctdoz
WORKDIR /data
# Encrypted mode: only /data/vault.enc touches disk; plaintext lives in the /work tmpfs (RAM).
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh", "--vault", "/data/vault.enc", "--work-dir", "/work"]
