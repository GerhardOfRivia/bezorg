# Static binaries from `make build`. The image has no Go toolchain.
# FTPS certificate checks use the system trust store installed below.
#   docker build -f contrib/container/Dockerfile -t bezorg .
FROM debian:stable-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY bin/bezorg bin/bezorgd /usr/local/bin/

ENV BEZORG_STATE_DIR=/var/lib/bezorg \
    BEZORG_SOCKET=/run/bezorg/bezorg.sock

WORKDIR /var/lib/bezorg
CMD ["bezorgd"]
