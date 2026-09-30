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

"""Regression coverage for 006_session_scoped_rls_policies.sql.

Every other RLS-adjacent test in this suite mocks the database connection
entirely, so none of them can actually prove row-level security enforces
anything -- they can only prove which SQL string gets sent. That gap is
exactly how the bug this migration fixes went unnoticed: the old
``TO <username>`` policies looked correct at the SQL-authoring level and
simply never matched a real session, for any user, ever.

test_datastream_rls_filters_by_network_per_user connects to a real
database and proves enforcement itself: that two sessions sharing the
same PostgreSQL group role ("user") but different individual identities
see different, correctly filtered rows. That per-user differentiation
*without* an individual login role is the entire point of the fix (see
the migration's own header comment), and it is precisely what the
previous design could never achieve. If a future change reintroduces
per-username policies, or breaks the session-variable plumbing in
set_role(), this test fails.

test_static_policies_are_group_scoped_not_per_user asserts the same thing
more directly and much more cheaply: every RLS policy on Datastream must
be scoped TO a shared group role, never TO an individual username.

Both skip cleanly (not a failure) if no database is reachable.

Each test opens its own asyncpg connection rather than reusing the app's
module-level get_pool() singleton. That pool is bound to whichever event
loop first created it; since each test here calls asyncio.run() (matching
the plain-function style the rest of this suite already uses -- no
pytest-asyncio fixtures), every test gets a fresh event loop, and a pool
opened on one loop cannot be reused once that loop closes. Confirmed
live: sharing get_pool() across two such tests raised
"cannot perform operation: another operation is in progress" on the
second test, purely from loop reuse -- unrelated to RLS itself.

Everything this module writes lives inside one transaction per test that
is always rolled back, never committed -- deliberately, not just for
hygiene. A real cleanup DELETE on sensorthings."User" was tried first and
found to be impossible for *any* row: AuditLog_actor_id_fkey is
ON DELETE SET NULL, and Postgres runs that enforcement trigger with the
referenced table's *owner* privileges (confirmed: both User and AuditLog
are owned by "administrator"), not the caller's -- and administrator was
deliberately never granted UPDATE on AuditLog, since it is meant to be
genuinely append-only. That makes DELETE FROM sensorthings."User" fail
for every caller, unconditionally, including the real DELETE /Users
endpoint -- a separate, real bug, logged in UNFINISHED_WORK.md rather
than fixed here. Rollback-only setup sidesteps it entirely for this
test's purposes without papering over it.
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

_MARKER = f"_rls_test_{uuid.uuid4().hex[:8]}"


async def _connect_or_skip():
    """A single connection scoped to this test's own asyncio.run() call --
    not the app's shared get_pool(), which is bound to whatever event loop
    created it. Same DSN shape as app.db.asyncpg_db.get_pool().
    """
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for RLS enforcement test: {exc}")


async def _seed(connection):
    """Two viewer users, each scoped to a different real Network, plus one
    existing Datastream from each of those Networks.

    A user's scope is the Network *name* held in User.dataset_id; the RLS
    policies resolve it through current_app_user_network_id(). No Datastream
    is inserted -- the seed data already has rows in several Networks, and
    reusing them avoids faking every NOT NULL FK.

    Switches to the administrator PG role first, same as every real
    privileged write in this codebase.
    """
    await set_role(connection, {"role": "administrator"})

    nets = await connection.fetch(
        """
        SELECT n.name, min(d.id) AS ds_id
        FROM sensorthings."Network" n
        JOIN sensorthings."Datastream" d ON d.network_id = n.id
        GROUP BY n.name
        ORDER BY n.name
        LIMIT 2;
        """
    )
    if len(nets) < 2:
        pytest.skip(
            "need two Networks that each have a Datastream -- run dummy_data "
            "with NETWORK=1"
        )
    (net_a, ds_a), (net_b, ds_b) = (
        (nets[0]["name"], nets[0]["ds_id"]),
        (nets[1]["name"], nets[1]["ds_id"]),
    )

    user_a = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role, dataset_id)
        VALUES ($1, 'viewer', $2) RETURNING id;
        """,
        f"{_MARKER}_alice",
        net_a,
    )
    user_b = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role, dataset_id)
        VALUES ($1, 'viewer', $2) RETURNING id;
        """,
        f"{_MARKER}_bob",
        net_b,
    )
    return user_a, user_b, ds_a, ds_b


def test_datastream_rls_filters_by_network_per_user():
    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                user_a, user_b, ds_a, ds_b = await _seed(connection)

                # Nested transaction (asyncpg issues a SAVEPOINT here,
                # since one is already open) so each SET LOCAL ROLE gets
                # its own boundary, exactly like set_role() being called
                # once per real request -- but all inside the same outer
                # transaction, so a single ROLLBACK at the end undoes
                # everything without ever needing DELETE.
                async with connection.transaction():
                    await set_role(connection, {"role": "viewer", "id": user_a})
                    visible = await connection.fetch(
                        'SELECT id FROM sensorthings."Datastream" '
                        "WHERE id = ANY($1::bigint[]);",
                        [ds_a, ds_b],
                    )
                    visible_ids = {r["id"] for r in visible}

                assert visible_ids == {ds_a}, (
                    "viewer session for user_a must see only its own "
                    f"Network's Datastream ({ds_a}), not {ds_b} -- got "
                    f"{visible_ids}. If this includes both ids, per-Network "
                    "filtering has broken; if it's empty, the static "
                    "policy itself stopped matching."
                )

                async with connection.transaction():
                    await set_role(connection, {"role": "viewer", "id": user_b})
                    visible = await connection.fetch(
                        'SELECT id FROM sensorthings."Datastream" '
                        "WHERE id = ANY($1::bigint[]);",
                        [ds_a, ds_b],
                    )
                    visible_ids = {r["id"] for r in visible}

                assert visible_ids == {ds_b}, (
                    f"viewer session for user_b must see only {ds_b}, not "
                    f"{ds_a} -- got {visible_ids}. Two users sharing the "
                    "same PostgreSQL group role ('user') seeing each "
                    "other's rows is exactly the bug "
                    "006_session_scoped_rls_policies.sql fixed."
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_static_policies_are_group_scoped_not_per_user():
    """Direct assertion against the exact shape of the original bug: every
    RLS policy on Datastream must be scoped TO a shared PostgreSQL group
    role ('user', 'sensor', 'qc', ...), never TO an individual username.
    A regression back to per-user policies would pass every mocked SQL-
    dispatch test in this suite while silently reintroducing a policy that
    can never match a real session -- this is the one check that would
    actually catch that. Read-only; needs no setup or cleanup.
    """

    async def _run():
        connection = await _connect_or_skip()
        try:
            rows = await connection.fetch(
                """
                SELECT policyname, roles AS role_names
                FROM pg_policies
                WHERE schemaname = 'sensorthings' AND tablename = 'Datastream';
                """
            )
        finally:
            await connection.close()

        assert rows, "expected at least one RLS policy on sensorthings.Datastream"

        # The complete, real set of PostgreSQL group roles this schema
        # defines (confirmed against pg_roles) that could legitimately
        # back an RLS policy. Application-layer role names like "custom"
        # are deliberately absent -- DB_ROLE_BY_RBAC_ROLE maps them onto a
        # shared PG group role, they have no PG role of their own
        # (see rbac_roles.py).
        known_group_roles = {"user", "sensor", "qc", "administrator", "guest", "public"}
        for row in rows:
            for role_name in row["role_names"]:
                assert role_name in known_group_roles, (
                    f"policy {row['policyname']!r} is scoped TO {role_name!r}, "
                    "which is not one of the known shared group roles -- "
                    "this looks like a per-username policy, the exact "
                    "pattern that never matched a real session before "
                    "006_session_scoped_rls_policies.sql."
                )

    asyncio.run(_run())
