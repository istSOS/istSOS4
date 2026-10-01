#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ENV_FILE="${ENV_FILE:-${SCRIPT_DIR}/.env}"

if [ ! -f "$ENV_FILE" ]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

IMAGE="${IMAGE_NAME:-ghcr.io/istsos/istsos4/utils/eyeonwater2istsos:0.1}"
DOCKER="${DOCKER:-/usr/bin/docker}"
LOOKBACK_DAYS="${EYEONWATER_LOOKBACK_DAYS:-2}"
BEGIN="${BEGIN:-${EYEONWATER_BEGIN:-}}"
END="${END:-${EYEONWATER_END:-}}"
BBOX="${BBOX:-${EYEONWATER_BBOX:-}}"

if [ -z "$BEGIN" ]; then
  BEGIN="$(date -u -d "${LOOKBACK_DAYS} days ago" '+%Y-%m-%dT%H:%M:%S')"
fi

ARGS=(--begin "$BEGIN")

if [ -n "$END" ]; then
  ARGS+=(--end "$END")
fi

if [ -n "$BBOX" ]; then
  ARGS+=(--bbox "$BBOX")
fi

"${DOCKER}" run --rm \
  --network host \
  --env-file "$ENV_FILE" \
  "${IMAGE}" \
  /app/fetch_eyeonwater2istsos.py \
  "${ARGS[@]}" \
  "$@"
