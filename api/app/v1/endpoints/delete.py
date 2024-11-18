import traceback

from app import AUTHORIZATION, DEBUG, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.oauth import get_current_user
from app.sta2rest import sta2rest
from app.v1.endpoints.update_patch import insertCommit
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse, Response

v1 = APIRouter()

try:
    DEBUG = DEBUG
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0

# Handle DELETE requests


@v1.api_route("/{path_name:path}", methods=["DELETE"])
async def catch_all_delete(
    request: Request,
    path_name: str,
    current_user=Depends(get_current_user) if AUTHORIZATION else None,
    pgpool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    """
    Delete endpoint for catching all DELETE requests.

    Args:
        request (Request): The incoming request object.
        path_name (str): The path name extracted from the URL.
        pgpool: The connection pool to the database.

    Returns:
        Response: The response object indicating the status of the delete operation.

    Raises:
        Exception: If no entity name or id is provided.

    """
    try:
        full_path = request.url.path
        result = sta2rest.STA2REST.parse_uri(full_path)

        headers = request.headers

        # Get main entity
        [name, id] = result["entity"]

        # Get the name and id
        if not name:
            raise Exception("No entity name provided")

        if not id:
            raise Exception("No entity id provided")

        async with pgpool.acquire() as conn:
            async with conn.transaction():

                if current_user is not None:
                    query = 'SET ROLE "{username}";'
                    await conn.execute(
                        query.format(username=current_user["username"])
                    )

                commit_id = None
                if VERSIONING:
                    if headers.get("commit-message"):
                        commit_message = headers.get("commit-message")
                        commit_author = (
                            current_user["uri"]
                            if current_user
                            and current_user["role"] != "sensor"
                            else "anonymous"
                        )
                        commit_encoding_type = "text/plain"
                        commit = {
                            "message": commit_message,
                            "author": commit_author,
                            "encodingType": commit_encoding_type,
                        }
                        commit_id = await insertCommit(commit, conn, "DELETE")
                        query = """
                            UPDATE sensorthings."{name}"
                            SET "commit_id" = $1
                            WHERE id = $2
                        """
                        await conn.execute(
                            query.format(name=name), commit_id, int(id)
                        )
                    else:
                        raise Exception("No commit message provided")

                id_deleted = None
                if name == "Observation":
                    delete_query = """
                        DELETE FROM sensorthings."Observation" 
                        WHERE id = $1 
                        RETURNING id, "phenomenonTime", "datastream_id";
                    """
                    res = await conn.fetchrow(delete_query, int(id))

                    if res:
                        id_deleted = res["id"]
                        obs_phenomenon_time = res["phenomenonTime"]
                        datastream_id = res["datastream_id"]

                        await update_datastream_phenomenon_time(
                            conn, obs_phenomenon_time, datastream_id
                        )

                        await update_datastream_observedArea(
                            conn, res["datastream_id"]
                        )
                elif name == "FeaturesOfInterest":
                    query = """
                        SELECT DISTINCT datastream_id
                        FROM sensorthings."Observation"
                        WHERE "featuresofinterest_id" = $1;
                    """
                    datastream_records = await conn.fetch(query, int(id))

                    for record in datastream_records:
                        ds_id = record["datastream_id"]
                        await update_datastream_observedArea(
                            conn, ds_id, int(id)
                        )

                    query = """
                        UPDATE sensorthings."Location"
                        SET "gen_foi_id" = NULL
                        WHERE "gen_foi_id" = $1;
                    """
                    await conn.execute(query, int(id))

                    query = f"""
                        DELETE FROM sensorthings."{name}"
                        WHERE id = $1
                        RETURNING id;
                    """
                    id_deleted = await conn.fetchval(query, int(id))
                    for record in datastream_records:
                        ds_id = record["datastream_id"]
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
                        await conn.execute(query, ds_id)

                else:
                    query = f"""
                        DELETE FROM sensorthings."{name}"
                        WHERE id = $1
                        RETURNING id;
                    """
                    id_deleted = await conn.fetchval(query, int(id))

                if id_deleted is None:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Nothing found.",
                        },
                    )
            if DEBUG:
                response2jsonfile(request, "", "requests.json")

            if current_user is not None:
                await conn.execute("RESET ROLE;")

            return Response(status_code=status.HTTP_200_OK)
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "code": 401,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )

    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


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


async def update_datastream_observedArea(conn, datastream_id, feature_id=None):
    async with conn.transaction():
        if feature_id is None:
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
            await conn.execute(query, datastream_id)
        else:
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
            await conn.execute(query, datastream_id, feature_id)
