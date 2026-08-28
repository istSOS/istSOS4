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

from app import EPSG, ST_AGGREGATE
from app.rbac_roles import DB_ROLE_BY_RBAC_ROLE
from app.utils.utils import pg_quote_ident

_PG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# PostgreSQL group role for the 'administrator' application role.
# Kept separate because 'administrator' is intentionally excluded from
# DB_ROLE_BY_RBAC_ROLE (it is a bootstrap-only role, not API-assignable).
_ADMIN_PG_ROLE = "administrator"


def _validate_role_identifier(role_name: str) -> str:
    """Validate that *role_name* is a safe PostgreSQL identifier.

    asyncpg does not support $1 placeholders for SET ROLE identifiers —
    PostgreSQL's SET ROLE only accepts a literal role name.  We therefore
    validate the identifier before interpolating it into the query string.

    Raises:
        ValueError: if the role_name does not match a plain PG identifier.
    """
    if not isinstance(role_name, str) or not _PG_IDENTIFIER_RE.match(role_name):
        raise ValueError("Invalid role identifier")
    return role_name


async def set_role(connection, current_user):
    """Switch to the correct PostgreSQL group role for this request.

    Maps the application-layer role (e.g. 'viewer', 'editor') to its
    underlying PostgreSQL group role (e.g. 'user', 'sensor') using
    ``DB_ROLE_BY_RBAC_ROLE``.

    Uses ``SET LOCAL ROLE`` which is **transaction-scoped**: it auto-reverts
    when the enclosing transaction commits or rolls back.  This eliminates
    the need for ``RESET ROLE`` calls and prevents pool leaks if a request
    is cancelled mid-stream.

    Must be called **inside** an open ``connection.transaction()`` block.
    """
    app_role = current_user.get("role", current_user.get("username"))
    pg_group_role = DB_ROLE_BY_RBAC_ROLE.get(app_role)
    if pg_group_role is None:
        # 'administrator' is not in DB_ROLE_BY_RBAC_ROLE — handle explicitly.
        if app_role == _ADMIN_PG_ROLE:
            pg_group_role = _ADMIN_PG_ROLE
        else:
            # Fallback: treat the value itself as the PG role name (e.g. 'guest').
            pg_group_role = app_role

    pg_group_role = _validate_role_identifier(pg_group_role)
    await connection.execute(f"SET LOCAL ROLE {pg_quote_ident(pg_group_role)};")


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
