#!/usr/bin/env bash

set -euo pipefail

FILE="${FILE:-${1:-}}"

if [ -z "$FILE" ]; then
  echo "Usage: FILE=/absolute/path/to/file.json utils/eyeonwater2istsos.sh [--thing-id ID] [--network-name NAME]" >&2
  echo "   or: utils/eyeonwater2istsos.sh /absolute/path/to/file.json [--thing-id ID] [--network-name NAME]" >&2
  exit 1
fi

if [ "${FILE:-}" = "${1:-}" ]; then
  shift
fi

if [ ! -f "$FILE" ]; then
  echo "File not found: $FILE" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
FILE_DIR="$(cd "$(dirname "$FILE")" && pwd -P)"
FILE_NAME="$(basename "$FILE")"
FILE="${FILE_DIR}/${FILE_NAME}"

docker run --rm \
  --network host \
  --env-file "${ROOT_DIR}/.env" \
  -v "${FILE}:${FILE}:ro" \
  -v "${ROOT_DIR}/utils/models.py:/app/models.py:ro" \
  -v "${ROOT_DIR}/utils/xlsx2istsos.py:/app/xlsx2istsos.py:ro" \
  -v "${ROOT_DIR}/utils/eyeonwater2istsos.py:/app/eyeonwater2istsos.py:ro" \
  --entrypoint python \
  xlsx2istsos \
  /app/eyeonwater2istsos.py \
  "$FILE" \
  "$@"
