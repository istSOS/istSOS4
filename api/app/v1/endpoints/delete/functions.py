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


async def set_commit(
    connection, commit_message, current_user, entity_name, entity_id
):
    if VERSIONING or AUTHORIZATION:
        commit_id = None
        if not commit_message:
            if current_user and current_user["role"] == "sensor":
                return await connection.fetchval(
                    """
                        SELECT id FROM sensorthings."Commit"
                        WHERE user_id = $1::bigint
                    """,
                    current_user["id"],
                )
            await connection.execute("RESET ROLE;")
            raise Exception("No commit message provided")

        commit = {
            "message": commit_message,
            "author": current_user["uri"] if current_user else "anonymous",
            "encodingType": "text/plain",
        }

        if current_user is not None:
            commit["user_id"] = current_user["id"]
            if current_user["role"] != "sensor":
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
                RETURNING id, "phenomenonTime", "datastream_id"; 
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
    conn, obs_phenomenon_time, datastream_id
):
    async with conn.transaction():
        query = """
            WITH datastream AS (
                SELECT "phenomenonTime"
                FROM sensorthings."Datastream"
                WHERE id = $1
            ),
            new_boundaries AS (
                SELECT
                    CASE
                        WHEN datastream."phenomenonTime" IS NOT NULL AND lower(datastream."phenomenonTime") = $2 THEN
                            (SELECT lower("phenomenonTime") FROM sensorthings."Observation" WHERE "datastream_id" = $1 ORDER BY "phenomenonTime" ASC LIMIT 1)
                        ELSE
                            lower(datastream."phenomenonTime")
                    END AS new_lower_bound,
                    CASE
                        WHEN datastream."phenomenonTime" IS NOT NULL AND upper(datastream."phenomenonTime") = $3 THEN
                            (SELECT upper("phenomenonTime") FROM sensorthings."Observation" WHERE "datastream_id" = $1 ORDER BY "phenomenonTime" DESC LIMIT 1)
                        ELSE
                            upper(datastream."phenomenonTime")
                    END AS new_upper_bound
                FROM datastream
            )
            UPDATE sensorthings."Datastream"
            SET "phenomenonTime" = 
                CASE
                    WHEN new_lower_bound IS NOT NULL AND new_upper_bound IS NOT NULL THEN tstzrange(new_lower_bound, new_upper_bound, '[]')
                    ELSE NULL
                END
            FROM new_boundaries
            WHERE id = $1;
        """
        await conn.execute(
            query,
            datastream_id,
            obs_phenomenon_time.lower,
            obs_phenomenon_time.upper,
        )


async def update_datastream_phenomenon_time_from_foi(connection, ds_id):
    async with connection.transaction():
        query = """
            WITH first_asc AS (
                SELECT "phenomenonTime"
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                ORDER BY "phenomenonTime" ASC
                LIMIT 1
            ),
            first_desc AS (
                SELECT "phenomenonTime"
                FROM sensorthings."Observation"
                WHERE "datastream_id" = $1
                ORDER BY "phenomenonTime" DESC
                LIMIT 1
            )
            UPDATE sensorthings."Datastream"
            SET "phenomenonTime" = 
                CASE
                    WHEN (SELECT "phenomenonTime" FROM first_asc) IS NOT NULL
                    AND (SELECT "phenomenonTime" FROM first_desc) IS NOT NULL
                    THEN tstzrange(
                        (SELECT lower("phenomenonTime") FROM first_asc),
                        (SELECT upper("phenomenonTime") FROM first_desc), 
                        '[]'
                    )
                    ELSE NULL
                END
            WHERE "id" = $1;
        """
        await connection.execute(query, ds_id)
