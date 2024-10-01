import traceback

from app import DEBUG, REDIS
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.sta2rest import sta2rest
from app.utils.utils import (
    update_datastream_observedArea_from_foi,
    update_datastream_observedArea_from_obs,
)
from fastapi.responses import JSONResponse, Response

from fastapi import APIRouter, Depends, Request, status

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
    request: Request, path_name: str, pgpool=Depends(get_pool)
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
        # parse uri
        result = sta2rest.STA2REST.parse_uri(full_path)

        # Get main entity
        [name, id] = result["entity"]

        # Get the name and id
        if not name:
            raise Exception("No entity name provided")

        if not id:
            raise Exception("No entity id provided")

        async with pgpool.acquire() as conn:
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

                    datastream_query = """
                        SELECT "phenomenonTime" 
                        FROM sensorthings."Datastream"
                        WHERE id = $1;
                    """
                    datastream_phenomenon_time = await conn.fetchval(
                        datastream_query, datastream_id
                    )
                    if datastream_phenomenon_time:
                        is_lower_match = (
                            obs_phenomenon_time
                            == datastream_phenomenon_time.lower
                        )
                        is_upper_match = (
                            obs_phenomenon_time
                            == datastream_phenomenon_time.upper
                        )

                        if is_lower_match or is_upper_match:
                            order_by = "ASC" if is_lower_match else "DESC"
                            observation_query = f"""
                                SELECT "phenomenonTime" 
                                FROM sensorthings."Observation"
                                WHERE "datastream_id" = $1
                                ORDER BY "phenomenonTime" {order_by}
                                LIMIT 1;
                            """
                            observation_phenomenon_time = await conn.fetchval(
                                observation_query, datastream_id
                            )
                            if observation_phenomenon_time:
                                if order_by == "ASC":
                                    new_lower_bound = (
                                        observation_phenomenon_time
                                    )
                                    new_upper_bound = (
                                        datastream_phenomenon_time.upper
                                    )
                                else:
                                    new_lower_bound = (
                                        datastream_phenomenon_time.lower
                                    )
                                    new_upper_bound = (
                                        observation_phenomenon_time
                                    )

                                update_datastream_query = """
                                    UPDATE sensorthings."Datastream"
                                    SET "phenomenonTime" = tstzrange($1, $2, '[]')
                                    WHERE id = $3;
                                """
                                await conn.execute(
                                    update_datastream_query,
                                    new_lower_bound,
                                    new_upper_bound,
                                    datastream_id,
                                )
                            else:
                                update_datastream_query = """
                                    UPDATE sensorthings."Datastream"
                                    SET "phenomenonTime" = NULL
                                    WHERE id = $1;
                                """
                                await conn.execute(
                                    update_datastream_query, datastream_id
                                )

                    await update_datastream_observedArea_from_obs(
                        conn, res["datastream_id"]
                    )
            else:
                if name == "FeaturesOfInterest":
                    await update_datastream_observedArea_from_foi(
                        conn, int(id), True
                    )

                query = f'DELETE FROM sensorthings."{name}" WHERE id = $1 RETURNING id'
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

        if REDIS:
            redis.flushall()

        return Response(status_code=status.HTTP_200_OK)

    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
