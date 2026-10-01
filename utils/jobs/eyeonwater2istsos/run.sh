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

if [ -n "${1:-}" ] && [[ "$1" != -* ]]; then
  FILE="$1"
  shift
else
  FILE="${EYEONWATER_JSON_PATH:-${FILE:-}}"
fi

if [ -z "$FILE" ]; then
  echo "Usage: EYEONWATER_JSON_PATH=/absolute/path/to/file.json $0 [--thing-id ID] [--network-name NAME]" >&2
  echo "   or: FILE=/absolute/path/to/file.json $0 [--thing-id ID] [--network-name NAME]" >&2
  echo "   or: $0 /absolute/path/to/file.json [--thing-id ID] [--network-name NAME]" >&2
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "File not found: $FILE" >&2
  exit 1
fi

FILE_DIR="$(cd "$(dirname "$FILE")" && pwd -P)"
FILE_NAME="$(basename "$FILE")"
FILE="${FILE_DIR}/${FILE_NAME}"
IMAGE="${IMAGE_NAME:-ghcr.io/istsos/istsos4/utils/eyeonwater2istsos:0.1}"
DOCKER="${DOCKER:-/usr/bin/docker}"

"${DOCKER}" run --rm \
  --network host \
  --env-file "$ENV_FILE" \
  -v "${FILE}:${FILE}:ro" \
  "${IMAGE}" \
  /app/eyeonwater2istsos.py \
  "$FILE" \
  "$@"
