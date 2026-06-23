#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
ENV_FILE="${ISTSOS4_UTILS_ENV_FILE:-${ROOT_DIR}/utils/.env}"

if [ ! -f "$ENV_FILE" ] && [ -f "${ROOT_DIR}/.env" ]; then
  ENV_FILE="${ROOT_DIR}/.env"
fi

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
  FILE="${XLSX_PATH:-${FILE:-}}"
fi

if [ -z "$FILE" ]; then
  echo "Usage: XLSX_PATH=/absolute/path/to/file.xlsx utils/xlsx2istsos.sh" >&2
  echo "   or: FILE=/absolute/path/to/file.xlsx utils/xlsx2istsos.sh" >&2
  echo "   or: utils/xlsx2istsos.sh /absolute/path/to/file.xlsx" >&2
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "File not found: $FILE" >&2
  exit 1
fi

FILE_DIR="$(cd "$(dirname "$FILE")" && pwd -P)"
FILE_NAME="$(basename "$FILE")"
FILE="${FILE_DIR}/${FILE_NAME}"
IMAGE="${ISTSOS4_UTILS_IMAGE:-istsos4-utils}"
DOCKER="${DOCKER:-/usr/bin/docker}"

"${DOCKER}" run --rm \
  --network host \
  --env-file "$ENV_FILE" \
  -v "${FILE}:${FILE}:ro" \
  "${IMAGE}" \
  /app/xlsx2istsos.py \
  "$FILE" \
  "$@"
