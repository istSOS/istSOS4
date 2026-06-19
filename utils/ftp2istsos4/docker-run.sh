#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="${IMAGE_NAME:-ghcr.io/istsos/istsos4/utils/ftp2istsos:0.1}"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/config.yaml}"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/logs}"
NETWORK_MODE="${NETWORK_MODE:-host}"
PULL_IMAGE="${PULL_IMAGE:-1}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "Configuration file not found: ${CONFIG_FILE}" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"

if [[ "${PULL_IMAGE}" == "1" ]]; then
    docker pull "${IMAGE_NAME}"
fi

docker_args=(
    run
    --rm
    --user "$(id -u):$(id -g)"
    -v "${CONFIG_FILE}:/config/config.yaml:ro"
    -v "${LOG_DIR}:/app/logs"
)

if [[ -t 0 && -t 1 ]]; then
    docker_args+=(-it)
fi

if [[ -n "${NETWORK_MODE}" ]]; then
    docker_args+=(--network "${NETWORK_MODE}")
fi

if [[ -d "${HOME}/.ssh" ]]; then
    docker_args+=(-v "${HOME}/.ssh:${HOME}/.ssh:ro")
fi

docker_args+=("${IMAGE_NAME}")

if [[ "$#" -gt 0 ]]; then
    docker_args+=("$@")
fi

exec docker "${docker_args[@]}"
