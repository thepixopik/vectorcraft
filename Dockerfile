# syntax=docker/dockerfile:1
FROM rust:1.95-slim-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --locked --release -p vectorcraft-cli

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fontconfig fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 vectorcraft \
    && mkdir -p /workspace \
    && chown -R vectorcraft:vectorcraft /workspace

COPY --from=builder /src/target/release/vectorcraft-cli /usr/local/bin/vectorcraft-cli
COPY --from=builder /src/assets/fonts /usr/share/fonts/truetype/vectorcraft
RUN fc-cache -f

USER vectorcraft
WORKDIR /workspace

ENV VECTORCRAFT_NO_PREFS=1
ENV PORT=7860
EXPOSE 7860

CMD ["sh", "-c", "exec vectorcraft-cli --safe-mode mcp --http 0.0.0.0:${PORT:-7860} --automation-read-root /workspace --automation-write-root /workspace"]
