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

"""Regression coverage for 010_observation_dataset_scoping.sql.

007_session_scoped_rls_policies.sql dataset-scoped Datastream for viewer,
editor, sensor, obs_manager, and qc, but left Observation on a blanket
``USING (TRUE)`` -- its own header comment calls this out as deferred
future work, not a deliberate design choice. Verified against the live
dev database before this migration: a viewer approved into one dataset
(2 of ~20 Datastreams) could still read all ~40k Observations across
every other dataset too -- the actual measurement values, not just the
Datastream metadata describing them.

This migration also closes the *write* side, which read-only testing
wouldn't catch: rbac_editor_write_observation, rbac_sensor_observation_insert,
and rbac_obs_manager_observation_all are independent RLS policies with
their own USING/WITH CHECK clauses -- scoping only the SELECT policy
would leave an editor able to blindly UPDATE/DELETE/INSERT an Observation
outside their own dataset even after they could no longer browse to it.

Same rollback-only, real-connection pattern as test_rls_enforcement.py,
for the same reason: this bug class (a policy that looks correct at the
SQL-authoring level) only shows up against a real RLS-enforcing session,
never against a mocked connection. Skips cleanly if no database is
reachable.
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

_MARKER = f"_obs_scope_test_{uuid.uuid4().hex[:8]}"


async def _connect_or_skip():
    try:
        return await asyncpg.connect(
            dsn=f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no reachable database for Observation-scoping test: {exc}")


async def _seed(connection, editor_role="viewer"):
    """Two datasets (A, B), one Datastream + one Observation cloned from
    real seed data in each, and one user in dataset A with the given role.
    Mirrors test_rls_enforcement.py's _seed() shape."""
    await set_role(connection, {"role": "administrator"})

    ds_template = await connection.fetchval(
        'SELECT id FROM sensorthings."Datastream" ORDER BY id LIMIT 1;'
    )
    obs_template = await connection.fetchval(
        'SELECT id FROM sensorthings."Observation" ORDER BY id LIMIT 1;'
    )
    if ds_template is None or obs_template is None:
        pytest.skip("no seed Datastream/Observation row to clone from")

    user_id = await connection.fetchval(
        """
        INSERT INTO sensorthings."User" (username, role, dataset_id)
        VALUES ($1, $2, $3) RETURNING id;
        """,
        f"{_MARKER}_user",
        editor_role,
        f"{_MARKER}_dataset_a",
    )

    ds_a = await connection.fetchval(
        """
        INSERT INTO sensorthings."Datastream"
            (name, description, "unitOfMeasurement", "observationType",
             thing_id, sensor_id, observedproperty_id, network_id, dataset_id)
        SELECT $1, description, "unitOfMeasurement", "observationType",
               thing_id, sensor_id, observedproperty_id, network_id, $2
        FROM sensorthings."Datastream" WHERE id = $3
        RETURNING id;
        """,
        f"{_MARKER}_ds_a",
        f"{_MARKER}_dataset_a",
        ds_template,
    )
    ds_b = await connection.fetchval(
        """
        INSERT INTO sensorthings."Datastream"
            (name, description, "unitOfMeasurement", "observationType",
             thing_id, sensor_id, observedproperty_id, network_id, dataset_id)
        SELECT $1, description, "unitOfMeasurement", "observationType",
               thing_id, sensor_id, observedproperty_id, network_id, $2
        FROM sensorthings."Datastream" WHERE id = $3
        RETURNING id;
        """,
        f"{_MARKER}_ds_b",
        f"{_MARKER}_dataset_b",
        ds_template,
    )

    obs_a = await connection.fetchval(
        """
        INSERT INTO sensorthings."Observation"
            ("phenomenonTimeStart", "phenomenonTimeEnd", "resultTime",
             "resultType", "resultString", "resultNumber", "resultBoolean",
             "resultJSON", "resultQuality", "validTime", parameters,
             datastream_id, featuresofinterest_id)
        SELECT "phenomenonTimeStart", "phenomenonTimeEnd", "resultTime",
               "resultType", "resultString", "resultNumber", "resultBoolean",
               "resultJSON", "resultQuality", "validTime", parameters,
               $1, featuresofinterest_id
        FROM sensorthings."Observation" WHERE id = $2
        RETURNING id;
        """,
        ds_a,
        obs_template,
    )
    obs_b = await connection.fetchval(
        """
        INSERT INTO sensorthings."Observation"
            ("phenomenonTimeStart", "phenomenonTimeEnd", "resultTime",
             "resultType", "resultString", "resultNumber", "resultBoolean",
             "resultJSON", "resultQuality", "validTime", parameters,
             datastream_id, featuresofinterest_id)
        SELECT "phenomenonTimeStart", "phenomenonTimeEnd", "resultTime",
               "resultType", "resultString", "resultNumber", "resultBoolean",
               "resultJSON", "resultQuality", "validTime", parameters,
               $1, featuresofinterest_id
        FROM sensorthings."Observation" WHERE id = $2
        RETURNING id;
        """,
        ds_b,
        obs_template,
    )
    return user_id, ds_a, ds_b, obs_a, obs_b


def test_viewer_cannot_select_observation_outside_own_dataset():
    """Positive control (own dataset) + the actual regression check
    (cross-dataset denial), same call shape as C8's live curl proof."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                user_id, ds_a, ds_b, obs_a, obs_b = await _seed(
                    connection, editor_role="viewer"
                )

                async with connection.transaction():
                    await set_role(connection, {"role": "viewer", "id": user_id})
                    visible = await connection.fetch(
                        'SELECT id FROM sensorthings."Observation" '
                        "WHERE id = ANY($1::bigint[]);",
                        [obs_a, obs_b],
                    )
                    visible_ids = {r["id"] for r in visible}

                assert visible_ids == {obs_a}, (
                    "a viewer scoped to dataset A must see only that "
                    f"dataset's Observation ({obs_a}), never dataset B's "
                    f"({obs_b}) -- got {visible_ids}. If this includes "
                    "both ids, Observation is still unscoped by dataset; "
                    "if it's empty, the policy stopped matching even the "
                    "viewer's own dataset."
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_editor_cannot_update_observation_outside_own_dataset():
    """The write-side half of the fix -- RLS silently matching zero rows
    on an UPDATE looks identical to success unless the row's actual state
    is checked afterward, same shape as the earlier PATCH/RLS fix this
    session already covered for Datastream/Thing/etc."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                user_id, ds_a, ds_b, obs_a, obs_b = await _seed(
                    connection, editor_role="editor"
                )

                async with connection.transaction():
                    await set_role(connection, {"role": "editor", "id": user_id})
                    result = await connection.execute(
                        'UPDATE sensorthings."Observation" '
                        'SET "resultString" = $1 WHERE id = $2;',
                        "edited by out-of-dataset editor",
                        obs_b,
                    )

                assert result == "UPDATE 0", (
                    "an editor scoped to dataset A must not be able to "
                    f"update dataset B's Observation ({obs_b}) -- got "
                    f"{result!r} instead of 'UPDATE 0'."
                )

                await set_role(connection, {"role": "administrator"})
                unchanged = await connection.fetchval(
                    'SELECT "resultString" FROM sensorthings."Observation" '
                    "WHERE id = $1;",
                    obs_b,
                )
                assert unchanged != "edited by out-of-dataset editor", (
                    "dataset B's Observation was modified by an editor "
                    "scoped to dataset A -- the write policy is not "
                    "actually dataset-scoped."
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_editor_can_update_observation_inside_own_dataset():
    """Positive control -- guards against over-scoping: the same editor
    must still be able to write within their own dataset."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                user_id, ds_a, ds_b, obs_a, obs_b = await _seed(
                    connection, editor_role="editor"
                )

                async with connection.transaction():
                    await set_role(connection, {"role": "editor", "id": user_id})
                    result = await connection.execute(
                        'UPDATE sensorthings."Observation" '
                        'SET "resultString" = $1 WHERE id = $2;',
                        "edited by in-dataset editor",
                        obs_a,
                    )

                assert result == "UPDATE 1", (
                    "an editor must still be able to update an Observation "
                    f"in their own dataset -- got {result!r}."
                )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())


def test_sensor_insert_observation_rejected_for_wrong_dataset_datastream():
    """The INSERT/WITH CHECK half: a sensor targeting a Datastream outside
    their own dataset must be rejected, not silently accepted."""

    async def _run():
        connection = await _connect_or_skip()
        try:
            transaction = connection.transaction()
            await transaction.start()
            try:
                user_id, ds_a, ds_b, obs_a, obs_b = await _seed(
                    connection, editor_role="sensor"
                )

                foi_id = await connection.fetchval(
                    'SELECT featuresofinterest_id FROM sensorthings."Observation" '
                    "WHERE id = $1;",
                    obs_a,
                )
                result_type = await connection.fetchval(
                    'SELECT "resultType" FROM sensorthings."Observation" '
                    "WHERE id = $1;",
                    obs_a,
                )

                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await set_role(connection, {"role": "sensor", "id": user_id})
                        await connection.execute(
                            """
                            INSERT INTO sensorthings."Observation"
                                ("resultType", datastream_id, featuresofinterest_id)
                            VALUES ($1, $2, $3);
                            """,
                            result_type,
                            ds_b,
                            foi_id,
                        )
            finally:
                await transaction.rollback()
        finally:
            await connection.close()

    asyncio.run(_run())
