#!/usr/bin/env bash

set -euo pipefail

FILE="${FILE:-${1:-}}"

if [ -z "$FILE" ]; then
  echo "Usage: FILE=/absolute/path/to/file.xlsx utils/xlsx2istsos.sh" >&2
  echo "   or: utils/xlsx2istsos.sh /absolute/path/to/file.xlsx" >&2
  exit 1
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
  xlsx2istsos \
  "$FILE"
