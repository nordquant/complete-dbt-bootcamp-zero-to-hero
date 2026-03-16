#!/usr/bin/env bash
set -euo pipefail

REGISTRY="ghcr.io"
IMAGE_NAME="nordquant/complete-dbt-bootcamp-zero-to-hero/dbt-bootcamp-devcontainer"
IMAGE="${REGISTRY}/${IMAGE_NAME}"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0" 2>/dev/null || realpath "$0")")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATE_TAG=$(date +%Y.%m.%d)
SHA_TAG="sha-$(git -C "$REPO_ROOT" rev-parse --short HEAD)"

if [ -z "${NQ_GITHUB_TOKEN:-}" ]; then
  onepass
fi

echo "==> Logging in to GitHub Container Registry"
echo "$NQ_GITHUB_TOKEN" | docker login "$REGISTRY" -u nordquant --password-stdin

echo "==> Ensuring buildx builder exists"
docker buildx inspect multiplatform >/dev/null 2>&1 \
  || docker buildx create --name multiplatform --use
docker buildx use multiplatform

echo "==> Building and pushing for linux/amd64 + linux/arm64"
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file "$REPO_ROOT/.devcontainer/Dockerfile" \
  --tag "${IMAGE}:latest" \
  --tag "${IMAGE}:${SHA_TAG}" \
  --tag "${IMAGE}:${DATE_TAG}" \
  --push \
  "$REPO_ROOT"

echo "==> Done. Pushed tags:"
echo "    ${IMAGE}:latest"
echo "    ${IMAGE}:${SHA_TAG}"
echo "    ${IMAGE}:${DATE_TAG}"
