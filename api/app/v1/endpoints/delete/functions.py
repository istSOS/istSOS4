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

from app import AUTHORIZATION, VERSIONING
from app.v1.endpoints.functions import insert_commit
from app.v1.endpoints.exceptions import BadRequest, Forbidden


async def set_commit(
    connection, commit_message, current_user, entity_name, entity_id
):
    if not (VERSIONING or AUTHORIZATION):
        return

    if current_user and current_user["role"] == "sensor":
        if commit_message:
            raise Forbidden("Sensor cannot provide commit message")
        return

    if not commit_message:
        raise BadRequest("No commit message provided")

    commit = {
        "message": commit_message,
        "author": current_user["uri"] if current_user else "anonymous",
        "encodingType": "text/plain",
    }

    if current_user is not None:
        commit["user_id"] = current_user["id"]
        commit_id = await insert_commit(connection, commit, "DELETE")
        query = f"""
            UPDATE sensorthings."{entity_name}"
            SET "commit_id" = $1
            WHERE id = $2
        """
        await connection.execute(query, commit_id, entity_id)


async def delete_entity(connection, entity_name, entity_id, obs=False):
    async with connection.transaction():
        if obs:
            return await connection.fetchrow(
                f"""
                    DELETE FROM sensorthings."{entity_name}"
                    WHERE id = $1
                    RETURNING id, "phenomenonTimeStart", "phenomenonTimeEnd", "resultTime", "datastream_id";
                """,
                entity_id,
            )
        return await connection.fetchval(
            f"""
                DELETE FROM sensorthings."{entity_name}"
                WHERE id = $1
                RETURNING id;
            """,
            entity_id,
        )


async def unlink_foi_from_location(connection, feature_of_interest_id):
    async with connection.transaction():
        query = """
            UPDATE sensorthings."Location"
            SET "gen_foi_id" = NULL
            WHERE "gen_foi_id" = $1;
        """
        await connection.execute(query, feature_of_interest_id)


async def update_datastream_phenomenon_time(
    conn,
    obs_phenomenon_start,
    obs_phenomenon_end,
    datastream_id,
    obs_result_time=None,
):
    async with conn.transaction():
        query = """
            WITH datastream AS (
                SELECT "phenomenonTime", "resultTime"
                FROM sensorthings."Datastream"
                WHERE id = $1
            ),
            new_boundaries AS (
                SELECT
                    CASE
                        WHEN datastream."phenomenonTime" IS NOT NULL AND lower(datastream."phenomenonTime") = $2 THEN
                            (SELECT "phenomenonTimeStart" FROM sensorthings."Observation" WHERE "datastream_id" = $1 ORDER BY "phenomenonTimeStart" ASC LIMIT 1)
                        ELSE
                            lower(datastream."phenomenonTime")
                    END AS new_ph_lower_bound,
                    CASE
                        WHEN datastream."phenomenonTime" IS NOT NULL AND upper(datastream."phenomenonTime") = $3 THEN
                            (SELECT "phenomenonTimeEnd" FROM sensorthings."Observation" WHERE "datastream_id" = $1 ORDER BY "phenomenonTimeEnd" DESC LIMIT 1)
                        ELSE
                            upper(datastream."phenomenonTime")
                    END AS new_ph_upper_bound,
                    CASE
                        WHEN datastream."resultTime" IS NOT NULL AND lower(datastream."resultTime") = $4 THEN
                            (SELECT "resultTime" FROM sensorthings."Observation" WHERE "datastream_id" = $1 AND "resultTime" IS NOT NULL ORDER BY "resultTime" ASC LIMIT 1)
                        ELSE
                            lower(datastream."resultTime")
                    END AS new_rt_lower_bound,
                    CASE
                        WHEN datastream."resultTime" IS NOT NULL AND upper(datastream."resultTime") = $4 THEN
                            (SELECT "resultTime" FROM sensorthings."Observation" WHERE "datastream_id" = $1 AND "resultTime" IS NOT NULL ORDER BY "resultTime" DESC LIMIT 1)
                        ELSE
                            upper(datastream."resultTime")
                    END AS new_rt_upper_bound
                FROM datastream
            )
            UPDATE sensorthings."Datastream"
            SET "phenomenonTime" =
                CASE
                    WHEN new_ph_lower_bound IS NOT NULL AND new_ph_upper_bound IS NOT NULL THEN tstzrange(new_ph_lower_bound, new_ph_upper_bound, '[]')
                    ELSE NULL
                END,
                "resultTime" =
                CASE
                    WHEN new_rt_lower_bound IS NOT NULL AND new_rt_upper_bound IS NOT NULL THEN tstzrange(new_rt_lower_bound, new_rt_upper_bound, '[]')
                    ELSE NULL
                END
            FROM new_boundaries
            WHERE id = $1;
        """
        await conn.execute(
            query,
            datastream_id,
            obs_phenomenon_start,
            obs_phenomenon_end,
            obs_result_time,
        )


async def update_datastream_phenomenon_time_from_foi(connection, ds_id):
    async with connection.transaction():
        query = """
            WITH first_asc_ph AS (
                SELECT "phenomenonTimeStart" AS ph
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                ORDER BY "phenomenonTimeStart" ASC
                LIMIT 1
            ),
            first_desc_ph AS (
                SELECT "phenomenonTimeEnd" AS ph
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                ORDER BY "phenomenonTimeEnd" DESC
                LIMIT 1
            ),
            first_asc_rt AS (
                SELECT "resultTime"
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                AND "resultTime" IS NOT NULL
                ORDER BY "resultTime" ASC
                LIMIT 1
            ),
            first_desc_rt AS (
                SELECT "resultTime"
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                AND "resultTime" IS NOT NULL
                ORDER BY "resultTime" DESC
                LIMIT 1
            )
            UPDATE sensorthings."Datastream"
            SET "phenomenonTime" =
                CASE
                    WHEN (SELECT ph FROM first_asc_ph) IS NOT NULL
                    AND (SELECT ph FROM first_desc_ph) IS NOT NULL
                    THEN tstzrange(
                        (SELECT ph FROM first_asc_ph),
                        (SELECT ph FROM first_desc_ph),
                        '[]'
                    )
                    ELSE NULL
                END,
                "resultTime" =
                CASE
                    WHEN (SELECT "resultTime" FROM first_asc_rt) IS NOT NULL
                    AND (SELECT "resultTime" FROM first_desc_rt) IS NOT NULL
                    THEN tstzrange(
                        (SELECT "resultTime" FROM first_asc_rt),
                        (SELECT "resultTime" FROM first_desc_rt),
                        '[]'
                    )
                    ELSE NULL
                END
            WHERE "id" = $1;
        """
        await connection.execute(query, ds_id)
