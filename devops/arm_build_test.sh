#!/usr/bin/env bash

docker buildx build \
  --build-arg VERSION="UNKNOWN" \
  --build-arg BUILD_DATE="NOW" \
  --build-arg BASE_IMAGE="ghcr.io/tvheadend/tvheadend:edge-debian" \
  --file docker/Dockerfile \
  --platform linux/arm64 \
  -t ghcr.io/headendarr/headendarr:latest \
  .
