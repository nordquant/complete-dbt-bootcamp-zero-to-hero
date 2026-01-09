#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

# Note: devcontainer.yml is skipped because act bind-mounts .venv which causes path issues
act push \
    --matrix python-version:3.13 \
    --matrix runs-on:ubuntu-latest-arm64 \
    -P ubuntu-latest-arm64=catthehacker/ubuntu:act-latest \
    -s PROFILES_YML="$(cat ./airbnb/profiles.yml)" \
    --env run_full_tests=true \
    --container-architecture linux/arm64 \
    -W .github/workflows/python-app.yml
