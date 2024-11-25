import json
import traceback
from datetime import datetime, timezone

import ujson
from app import (
    AUTHORIZATION,
    COUNT_ESTIMATE_THRESHOLD,
    COUNT_MODE,
    HOSTNAME,
    PARTITION_CHUNK,
    REDIS,
    SUBPATH,
    VERSION,
    VERSIONING,
)
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.oauth import get_current_user
from app.settings import serverSettings, tables
from app.sta2rest import sta2rest
from app.utils.utils import build_nextLink
from app.v1.endpoints.functions import set_role
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from .query_parameters import CommonQueryParams, get_common_query_params

v1 = APIRouter()


user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


def __handle_root():
    """
    Handle the root path.

    Args:
        request (Request): The incoming request object.

    Returns:
        dict: The response containing the value and server settings.
    """
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": f"{HOSTNAME}{SUBPATH}{VERSION}" + "/" + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    }
    return response


async def wrapped_result_generator(first_item, result):
    yield first_item
    async for item in result:
        yield item


@v1.api_route(
    "/{path_name:path}",
    methods=["GET"],
    tags=["Catch All"],
    summary="Catch all GET requests",
    description="Handles all GET requests to the API",
)
async def catch_all_get(
    request: Request,
    path_name: str,
    current_user=user,
    pool=Depends(get_pool),
    params: CommonQueryParams = Depends(get_common_query_params),
):

    if not path_name:
        return __handle_root()

    try:
        full_path = request.url.path
        if request.url.query:
            full_path += "?" + request.url.query

        data = None

        if REDIS:
            result = redis.get(full_path)
            if result:
                data = json.loads(result)
                print("Cache hit")
            else:
                print("Cache miss")

        if not data:
            data = sta2rest.STA2REST.convert_query(full_path)

        main_entity = data.get("main_entity")
        main_query = data.get("main_query")
        top_value = data.get("top_value")
        is_count = data.get("is_count")
        count_queries = data.get("count_queries")
        as_of_value = data.get("as_of_value")
        from_to_value = data.get("from_to_value")
        single_result = data.get("single_result")

        result = asyncpg_stream_results(
            main_entity,
            main_query,
            pool,
            top_value,
            is_count,
            count_queries,
            as_of_value,
            from_to_value,
            single_result,
            full_path,
            current_user,
        )

        try:
            first_item = await anext(result)
            return StreamingResponse(
                wrapped_result_generator(first_item, result),
                media_type="application/json",
                status_code=status.HTTP_200_OK,
            )
        except Exception as e:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={
                    "code": 404,
                    "type": "error",
                    "message": "Not Found",
                },
            )

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def asyncpg_stream_results(
    entity,
    query,
    pgpool,
    top,
    is_count,
    count_queries,
    as_of_value,
    from_to_value,
    single_result,
    full_path,
    current_user,
):
    async with pgpool.acquire() as connection:
        async with connection.transaction():
            if current_user is not None:
                await set_role(connection, current_user)

            if is_count:
                if COUNT_MODE == "LIMIT_ESTIMATE":
                    query_count = await connection.fetchval(count_queries[0])
                    if query_count == COUNT_ESTIMATE_THRESHOLD:
                        query_count = await connection.fetchval(
                            "SELECT sensorthings.count_estimate($1) AS estimated_count",
                            count_queries[1],
                        )
                elif COUNT_MODE == "ESTIMATE_LIMIT":
                    query_count = await connection.fetchval(
                        "SELECT sensorthings.count_estimate($1) AS estimated_count",
                        count_queries[0],
                    )
                    if query_count < COUNT_ESTIMATE_THRESHOLD:
                        query_count = await connection.fetchval(
                            count_queries[1]
                        )
                else:
                    query_count = await connection.fetchval(count_queries[0])

            iot_count = (
                '"@iot.count": ' + str(query_count) + ","
                if is_count and not single_result
                else ""
            )
            await connection.execute(f"DECLARE my_cursor CURSOR FOR {query}")

            start_json = ""
            is_first_partition = True
            has_rows = False

            if VERSIONING:
                as_of_value = (
                    as_of_value
                    if as_of_value is not None
                    else datetime.now(timezone.utc)
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )

            while True:
                partition = await connection.fetch(
                    f"FETCH {PARTITION_CHUNK} FROM my_cursor"
                )
                if not partition:
                    break

                partition_len = len(partition)
                has_rows = True

                if partition_len > top - 1:
                    partition = partition[:-1]

                if (
                    VERSIONING
                    and single_result
                    and partition_len == 1
                    and entity != "Commit"
                    and not from_to_value
                ):
                    partition_data = ujson.loads(partition[0]["json"])
                    partition_data["@iot.as_of"] = as_of_value
                    partition_json = ujson.dumps(
                        partition_data,
                        escape_forward_slashes=False,
                    )
                else:
                    processed_partition = [
                        ujson.loads(record["json"]) for record in partition
                    ]
                    partition_json = ujson.dumps(
                        processed_partition,
                        escape_forward_slashes=False,
                    )[1:-1]

                if is_first_partition:
                    if partition_len > 0 and not single_result:
                        start_json = "{"

                    next_link = build_nextLink(full_path, partition_len)
                    next_link_json = (
                        f'"@iot.nextLink": "{next_link}",'
                        if next_link and not single_result
                        else ""
                    )
                    as_of = (
                        f'"@iot.as_of": "{as_of_value}",'
                        if VERSIONING
                        and not single_result
                        and not from_to_value
                        else ""
                    )
                    start_json += as_of + iot_count + next_link_json
                    start_json += (
                        '"value": ['
                        if (partition_len > 0 and not single_result)
                        else ""
                    )

                    yield start_json + partition_json
                    is_first_partition = False
                else:
                    yield "," + partition_json

            if not has_rows and not single_result:
                yield '{"value": []}'

            if has_rows and not single_result:
                yield "]}"

            await connection.execute("CLOSE my_cursor")

            if current_user is not None:
                await connection.execute("RESET ROLE")
