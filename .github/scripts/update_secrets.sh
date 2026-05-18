#!/bin/bash
# ============================================================================
# NOT COURSEWARE — COURSEWARE DEVELOPMENT INFRASTRUCTURE
# This script is part of the courseware development/CI infrastructure and is
# NOT part of the bootcamp course material. Students should ignore it.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROFILES_FILE="$REPO_ROOT/airbnb/profiles.yml"

if [ ! -f "$PROFILES_FILE" ]; then
    echo "Error: $PROFILES_FILE not found" >&2
    exit 1
fi

gh secret set PROFILES_YML < "$PROFILES_FILE"
echo "Uploaded $PROFILES_FILE as PROFILES_YML"
