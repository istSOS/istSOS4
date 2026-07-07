#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="${IMAGE_NAME:-ghcr.io/istsos/istsos4/utils/ftp2istsos:0.3}"
PUSH_IMAGE="${PUSH_IMAGE:-0}"

docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

if [[ "${PUSH_IMAGE}" == "1" ]]; then
    docker push "${IMAGE_NAME}"
fi
