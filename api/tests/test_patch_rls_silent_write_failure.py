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

"""Regression coverage for the "PATCH silently returns 200 even when RLS
blocks the write" bug (see UNFINISHED_WORK.md).

Every mocked test for the update/* endpoints can only prove which SQL
string got sent, not whether it actually changed a row -- the same gap
that let this bug hide in the first place. RLS doesn't raise an error
when an UPDATE's policy excludes a row; it just quietly matches zero rows
and reports success, so this needs a real database and a real RLS-scoped
session to reproduce.

update_entity() (api/app/v1/endpoints/update/functions.py) now returns
True/False for whether the UPDATE actually matched a row (obs=False path)
or the RETURNING row / None (obs=True). A viewer role shares Thing's
SELECT policy with editor (rbac_user_select_thing, USING (TRUE)) but not
its write policy (rbac_editor_write_thing, USING (current_app_user_role()
= 'editor')) -- so a viewer session can see a Thing but not write it,
which is exactly the "check_id_exists succeeds, the UPDATE silently
touches zero rows" scenario the bug was about.

Same rollback-only setup/connection pattern as test_rls_enforcement.py,
for the same reason: a real cleanup DELETE on sensorthings."User" is
impossible for any row (AuditLog_actor_id_fkey trigger runs as the
table owner, which was deliberately never granted UPDATE on AuditLog).
Skips cleanly (not a failure) if no database is reachable.
"""

import asyncio
import os
import sys
import uuid
from pathlib import Path

import pytest

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "admin")
os.environ.setdefault("POSTGRES_HOST", "database")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")

import asyncpg  # noqa: E402

from app import (  # noqa: E402
    ISTSOS_ADMIN,
    ISTSOS_ADMIN_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)
from app.v1.endpoints.functions import set_role  # noqa: E402
from app.v1.endpoints.update.functions import update_thing_entity  # noqa: E402

_MARKER = f"_patch_rls_test_{uuid.uuid4().hex[:8]}"


async def _connect_or_skip():
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for PATCH/RLS test: {exc}")


async def _seed(connection):
    """A viewer, an editor, and a Thing row -- both roles can SELECT it
    (rbac_user_select_thing has no dataset scoping), only editor can
    write it (rbac_editor_write_thing)."""
    await set_role(connection, {"role": "administrator"})

    viewer_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role)
        VALUES ($1, 'viewer') RETURNING id;
        """,
        f"{_MARKER}_viewer",
    )
    editor_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role)
        VALUES ($1, 'editor') RETURNING id;
        """,
        f"{_MARKER}_editor",
    )
    commit_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."Commit" (message, author, "encodingType", "actionType", user_id)
        VALUES ('seed', 'test', 'text/plain', 'CREATE', $1) RETURNING id;
        """,
        editor_id,
    )
    thing_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."Thing" (name, description, properties, commit_id)
        VALUES ($1, 'original description', '{}'::jsonb, $2) RETURNING id;
        """,
        f"{_MARKER}_thing",
        commit_id,
    )
    return viewer_id, editor_id, thing_id


def test_viewer_patch_is_rejected_not_silently_dropped():
    """A viewer PATCHing a Thing must get an explicit False signal, and the
    row must be provably unchanged -- not a silent, misleading success."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                viewer_id, _editor_id, thing_id = await _seed(connection)

                async with connection.transaction():
                    await set_role(connection, {"role": "viewer", "id": viewer_id})

                    # Sanity: viewer really can see the row (this is the
                    # exact precondition that made the bug possible --
                    # check_id_exists succeeding gave no signal about the
                    # write itself).
                    visible = await connection.fetchval(
                        'SELECT id FROM sensorthings."Thing" WHERE id = $1;',
                        thing_id,
                    )
                    assert visible == thing_id

                    result = await update_thing_entity(
                        connection, thing_id, {"description": "viewer was here"}
                    )

                assert result is False, (
                    "update_thing_entity() must return False when RLS "
                    f"silently blocks the write, got {result!r} -- if this "
                    "is None or True, the caller has no way to tell a "
                    "real write from a no-op and will report a false 200."
                )

                await set_role(connection, {"role": "administrator"})
                description = await connection.fetchval(
                    'SELECT description FROM sensorthings."Thing" WHERE id = $1;',
                    thing_id,
                )
                assert description == "original description", (
                    "the viewer's blocked write must not have changed "
                    f"the row -- got {description!r}"
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_editor_patch_succeeds():
    """The positive control: an editor's PATCH must still actually write,
    proving the fix didn't just make every PATCH fail closed."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                _viewer_id, editor_id, thing_id = await _seed(connection)

                async with connection.transaction():
                    await set_role(connection, {"role": "editor", "id": editor_id})
                    result = await update_thing_entity(
                        connection, thing_id, {"description": "editor was here"}
                    )

                assert result is True

                await set_role(connection, {"role": "administrator"})
                description = await connection.fetchval(
                    'SELECT description FROM sensorthings."Thing" WHERE id = $1;',
                    thing_id,
                )
                assert description == "editor was here"
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_association_only_patch_returns_none_not_false():
    """A PATCH that only touches an association (no direct Thing columns)
    must return None, not False -- update_entity() is never called at all
    in this case, so there's nothing to have failed. Endpoint handlers key
    off `is False` specifically (not falsy) so this legitimate case is
    never mistaken for the RLS-blocked-write case."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                _viewer_id, editor_id, thing_id = await _seed(connection)

                async with connection.transaction():
                    await set_role(connection, {"role": "editor", "id": editor_id})
                    result = await update_thing_entity(connection, thing_id, {})

                assert result is None
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())
