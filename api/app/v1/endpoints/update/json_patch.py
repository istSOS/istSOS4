# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""JSON Patch (RFC 6902) support for entity PATCH endpoints.

18-088 §10.3 / req/create-update-delete/update-entity-jsonpatch (advertised in
serverSettings.conformance) requires that a PATCH carrying
``Content-Type: application/json-patch+json`` with a JSON array body be applied
as an RFC 6902 patch, while a plain JSON object body keeps the existing
JSON-Merge-Patch semantics.

The PATCH routes previously declared ``payload: dict = Body(...)``, which made
FastAPI reject the RFC 6902 array body with 422 before any handler logic ran.
``normalize_patch_body`` is used as a dependency in place of that ``Body`` so a
list body is accepted; ``apply_json_patch_to_entity`` then loads the targeted
entity columns, applies the operations to that working document and returns the
resulting merge dict, which feeds unchanged into the existing update path.
"""

import copy
import json

from fastapi import Request


class JsonPatchError(Exception):
    """A malformed or non-applicable RFC 6902 patch (client error)."""

    def __init__(self, message: str, status_code: int = 400):
        self.status_code = status_code
        super().__init__(message)


class JsonPatchPayload:
    """Marker wrapper for a parsed RFC 6902 operation array.

    Lets the handlers distinguish a JSON-Patch body from a merge-patch dict
    while keeping the rest of the request flow untouched.
    """

    def __init__(self, operations: list):
        self.operations = operations


JSON_PATCH_CONTENT_TYPE = "application/json-patch+json"


async def normalize_patch_body(request: Request):
    """Dependency that yields either a merge-patch dict or a JsonPatchPayload.

    A list body (or ``application/json-patch+json`` content type) is an RFC 6902
    patch; anything else is treated as today's JSON-Merge-Patch object body.
    """
    raw = await request.body()
    if not raw:
        return {}

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise JsonPatchError(f"Invalid JSON body: {exc}")

    content_type = request.headers.get("content-type", "").lower()
    if isinstance(data, list) or "json-patch" in content_type:
        if not isinstance(data, list):
            raise JsonPatchError(
                "A application/json-patch+json body must be an array of "
                "operations (RFC 6902)."
            )
        return JsonPatchPayload(data)

    return data


# ---------------------------------------------------------------------------
# RFC 6901 JSON Pointer helpers
# ---------------------------------------------------------------------------


def parse_pointer(pointer):
    """Parse an RFC 6901 JSON Pointer into a list of reference tokens."""
    if pointer == "":
        return []
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise JsonPatchError(f"Invalid JSON Pointer: {pointer!r}")
    # Unescape ~1 -> '/' and ~0 -> '~' (order matters per RFC 6901 §4).
    return [
        token.replace("~1", "/").replace("~0", "~")
        for token in pointer.split("/")[1:]
    ]


def list_index(array, token):
    """Resolve a JSON Pointer array token to a valid existing index."""
    if token == "-" or not token.lstrip("-").isdigit():
        raise JsonPatchError(f"Invalid array index: {token!r}")
    idx = int(token)
    if idx < 0 or idx >= len(array):
        raise JsonPatchError(f"Array index out of range: {token!r}")
    return idx


def get(document, tokens):
    current = document
    for token in tokens:
        if isinstance(current, dict):
            if token not in current:
                raise JsonPatchError(f"Path not found: /{'/'.join(tokens)}")
            current = current[token]
        elif isinstance(current, list):
            current = current[list_index(current, token)]
        else:
            raise JsonPatchError(f"Path not found: /{'/'.join(tokens)}")
    return current


def add_tokens(document, tokens, value):
    """RFC 6902 §4.1 add at the location addressed by ``tokens``."""
    if not tokens:
        return value  # add to "" replaces the whole document
    parent = get(document, tokens[:-1])
    last = tokens[-1]
    if isinstance(parent, dict):
        parent[last] = value
    elif isinstance(parent, list):
        if last == "-":
            parent.append(value)
        else:
            idx = int(last) if last.lstrip("-").isdigit() else None
            if idx is None or idx < 0 or idx > len(parent):
                raise JsonPatchError(f"Invalid array index for add: {last!r}")
            parent.insert(idx, value)
    else:
        raise JsonPatchError(
            f"Cannot add to a non-container at /{'/'.join(tokens[:-1])}"
        )
    return document


def remove_tokens(document, tokens):
    """RFC 6902 §4.2 remove at the location addressed by ``tokens``."""
    if not tokens:
        raise JsonPatchError("Cannot remove the whole document")
    parent = get(document, tokens[:-1])
    last = tokens[-1]
    if isinstance(parent, dict):
        if last not in parent:
            raise JsonPatchError(f"Cannot remove missing member: {last!r}")
        del parent[last]
    elif isinstance(parent, list):
        parent.pop(list_index(parent, last))
    else:
        raise JsonPatchError("Cannot remove from a non-container")
    return document


def replace_tokens(document, tokens, value):
    """RFC 6902 §4.3 replace; the target location MUST already exist."""
    if not tokens:
        return value
    parent = get(document, tokens[:-1])
    last = tokens[-1]
    if isinstance(parent, dict):
        if last not in parent:
            raise JsonPatchError(f"Cannot replace missing member: {last!r}")
        parent[last] = value
    elif isinstance(parent, list):
        parent[list_index(parent, last)] = value
    else:
        raise JsonPatchError("Cannot replace inside a non-container")
    return document


def apply_operation(document, op):
    if not isinstance(op, dict):
        raise JsonPatchError("Each JSON Patch operation must be an object")

    name = op.get("op")
    if name in ("add", "replace", "test") and "value" not in op:
        raise JsonPatchError(f"'{name}' operation requires a 'value' member")

    if name == "add":
        return add_tokens(document, parse_pointer(op["path"]), op["value"])

    if name == "remove":
        return remove_tokens(document, parse_pointer(op["path"]))

    if name == "replace":
        return replace_tokens(
            document, parse_pointer(op["path"]), op["value"]
        )

    if name in ("move", "copy"):
        from_tokens = parse_pointer(op["from"])
        path_tokens = parse_pointer(op["path"])
        value = get(document, from_tokens)
        if name == "move":
            # RFC 6902 §4.4: the "from" location MUST NOT be a proper prefix of
            # the "path" location (cannot move a value into one of its children).
            if from_tokens == path_tokens[: len(from_tokens)] and len(
                from_tokens
            ) < len(path_tokens):
                raise JsonPatchError("Cannot move a value into its own child")
            document = remove_tokens(document, from_tokens)
            return add_tokens(document, path_tokens, value)
        return add_tokens(document, path_tokens, copy.deepcopy(value))

    if name == "test":
        actual = get(document, parse_pointer(op["path"]))
        if actual != op["value"]:
            # RFC 6902 §4.6: a failed "test" aborts the whole patch.
            raise JsonPatchError("JSON Patch 'test' operation failed", 409)
        return document

    raise JsonPatchError(f"Unsupported JSON Patch operation: {name!r}")


def apply_patch(document, operations):
    """Apply a sequence of RFC 6902 operations to a copy of ``document``."""
    working = copy.deepcopy(document)
    for op in operations:
        working = apply_operation(working, op)
    return working


# ---------------------------------------------------------------------------
# Entity integration
# ---------------------------------------------------------------------------


def referenced_columns(operations):
    """Top-level entity columns touched by the patch (first pointer token)."""
    columns = set()
    for op in operations:
        if not isinstance(op, dict):
            raise JsonPatchError("Each JSON Patch operation must be an object")
        for key in ("path", "from"):
            pointer = op.get(key)
            if pointer is None:
                continue
            tokens = parse_pointer(pointer)
            if not tokens:
                raise JsonPatchError(
                    "JSON Patch targeting the whole entity ('') is not "
                    "supported; address a property instead."
                )
            columns.add(tokens[0])
    return columns


async def load_document(connection, entity_name, entity_id, columns):
    """Build the working JSON document from the entity's current columns.

    Only the columns referenced by the patch are read. JSON/JSONB columns are
    decoded to Python objects so the pointer ops can navigate into them.
    """
    type_rows = await connection.fetch(
        """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'sensorthings' AND table_name = $1
        """,
        entity_name,
    )
    column_types = {row["column_name"]: row["data_type"] for row in type_rows}

    existing = [col for col in columns if col in column_types]
    document = {}
    if existing:
        col_list = ", ".join(f'"{col}"' for col in existing)
        row = await connection.fetchrow(
            f'SELECT {col_list} FROM sensorthings."{entity_name}" WHERE id = $1',
            entity_id,
        )
        if row is not None:
            for col in existing:
                value = row[col]
                if isinstance(value, str) and column_types[col] in (
                    "json",
                    "jsonb",
                ):
                    value = json.loads(value)
                document[col] = value
    return document


async def apply_json_patch_to_entity(
    connection, entity_name, entity_id, payload
):
    """Normalize a PATCH body into a merge dict.

    Merge-patch (dict) bodies pass through unchanged. A JsonPatchPayload is
    applied (RFC 6902) against the entity's current column values and the
    touched top-level columns are returned as the merge dict consumed by the
    existing ``update_*_entity`` path.
    """
    if not isinstance(payload, JsonPatchPayload):
        return payload

    operations = payload.operations
    columns = referenced_columns(operations)
    document = await load_document(
        connection, entity_name, entity_id, columns
    )
    patched = apply_patch(document, operations)
    # Only the columns the patch touched are written back. A column removed at
    # the top level is set to NULL.
    return {col: patched.get(col, None) for col in columns}
