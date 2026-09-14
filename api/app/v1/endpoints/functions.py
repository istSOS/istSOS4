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

import json
import re

from app import ST_AGGREGATE
from app.utils.utils import pg_quote_ident

_PG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_role_identifier(username: str) -> str:
    if not isinstance(username, str) or not _PG_IDENTIFIER_RE.match(username):
        raise ValueError("Invalid role identifier")
    return username


async def set_role(connection, current_user):
    async with connection.transaction():
        username = validate_role_identifier(current_user["username"])
        query = f"SET ROLE {pg_quote_ident(username)};"
        await connection.execute(query)


async def insert_commit(connection, payload, action):
    """
    Inserts a commit record into the database.

    Args:
        payload (dict): Commit data.
        connection: The database connection.
        action (str): Action type (e.g., "DELETE").

    Returns:
        int: The ID of the inserted commit.
    """
    async with connection.transaction():
        payload["actionType"] = action

        for key in list(payload.keys()):
            if isinstance(payload[key], dict):
                payload[key] = json.dumps(payload[key])

        keys = ", ".join(f'"{key}"' for key in payload.keys())
        values_placeholders = ", ".join(f"${i+1}" for i in range(len(payload)))
        query = f"""
            INSERT INTO sensorthings."Commit" ({keys})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        return await connection.fetchval(query, *payload.values())


async def get_datastreams_from_foi(connection, feature_of_interest_id):
    async with connection.transaction():
        query = """
            SELECT DISTINCT datastream_id
            FROM sensorthings."Observation"
            WHERE featuresofinterest_id = $1;
        """
        return await connection.fetch(query, feature_of_interest_id)


async def update_datastream_observedArea(conn, datastream_id, feature_id=None):
    async with conn.transaction():
        if feature_id is None:
            if ST_AGGREGATE == "CONVEX_HULL":
                query = """
                    WITH distinct_features AS (
                        SELECT DISTINCT ON (foi.id) foi.feature
                        FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                        WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1
                    ),
                    aggregated_geometry AS (
                        SELECT ST_ConvexHull(ST_Collect(feature)) AS agg_geom
                        FROM distinct_features
                    )
                    UPDATE sensorthings."Datastream"
                    SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                    WHERE id = $1;
                """
            else:
                query = """
                    WITH distinct_features AS (
                        SELECT DISTINCT ON (foi.id) foi.feature
                        FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                        WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1
                    ),
                    aggregated_geometry AS (
                        SELECT ST_Envelope(ST_Collect(feature)) AS agg_geom
                        FROM distinct_features
                    )
                    UPDATE sensorthings."Datastream"
                    SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                    WHERE id = $1;
                """
            await conn.execute(query, datastream_id)
        else:
            if ST_AGGREGATE == "CONVEX_HULL":
                query = """
                    WITH distinct_features AS (
                        SELECT DISTINCT ON (foi.id) foi.feature
                        FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                        WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1 AND foi.id != $2
                    ),
                    aggregated_geometry AS (
                        SELECT ST_ConvexHull(ST_Collect(feature)) AS agg_geom
                        FROM distinct_features
                    )
                    UPDATE sensorthings."Datastream"
                    SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                    WHERE id = $1;
                """
            else:
                query = """
                    WITH distinct_features AS (
                        SELECT DISTINCT ON (foi.id) foi.feature
                        FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                        WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1 AND foi.id != $2
                    ),
                    aggregated_geometry AS (
                        SELECT ST_Envelope(ST_Collect(feature)) AS agg_geom
                        FROM distinct_features
                    )
                    UPDATE sensorthings."Datastream"
                    SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
                    WHERE id = $1;
                """
            await conn.execute(query, datastream_id, feature_id)


async def update_datastream_time_ranges(
    conn,
    datastream_id,
    phenomenon_time_start,
    phenomenon_time_end,
    result_time_start=None,
    result_time_end=None,
):
    """Widen a Datastream's phenomenonTime/resultTime to cover an Observation.

    LEAST/GREATEST ignore NULL arguments, so a range that is still NULL is
    initialised from the observation's own instants instead of staying NULL.
    A pair of NULL bounds leaves its range untouched.
    """
    query = """
        UPDATE sensorthings."Datastream"
        SET "phenomenonTime" =
                CASE
                    WHEN $2::timestamptz IS NULL OR $3::timestamptz IS NULL
                        THEN "phenomenonTime"
                    ELSE tstzrange(
                        LEAST($2::timestamptz, lower("phenomenonTime")),
                        GREATEST($3::timestamptz, upper("phenomenonTime")),
                        '[]'
                    )
                END,
            "resultTime" =
                CASE
                    WHEN $4::timestamptz IS NULL OR $5::timestamptz IS NULL
                        THEN "resultTime"
                    ELSE tstzrange(
                        LEAST($4::timestamptz, lower("resultTime")),
                        GREATEST($5::timestamptz, upper("resultTime")),
                        '[]'
                    )
                END
        WHERE id = $1::bigint;
    """
    await conn.execute(
        query,
        datastream_id,
        phenomenon_time_start,
        phenomenon_time_end,
        result_time_start,
        result_time_end,
    )
