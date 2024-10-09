import json
import traceback
from datetime import datetime

import ujson
from app import (
    COUNT_ESTIMATE_THRESHOLD,
    COUNT_MODE,
    DEBUG,
    HOSTNAME,
    PARTITION_CHUNK,
    REDIS,
    STREAMING_MODE,
    SUBPATH,
    VERSION,
    VERSIONING,
)
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.db.sqlalchemy_db import get_db
from app.settings import serverSettings, tables
from app.sta2rest import sta2rest
from app.sta2rest.visitors import stream_results
from app.utils.utils import build_nextLink
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.exc import TimeoutError
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from fastapi import APIRouter, Depends, Request, status

v1 = APIRouter()

try:
    DEBUG = DEBUG
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0


def __handle_root(request: Request):
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
    if DEBUG:
        response2jsonfile(request, response, "requests.json")
    return response


@v1.api_route("/{path_name:path}", methods=["GET"])
async def catch_all_get(
    request: Request,
    path_name: str,
    db_sqlalchemy: Session = Depends(get_db),
    pgpool=Depends(get_pool),
):
    """
    Handle GET requests for all paths.

    Args:
        request (Request): The incoming request object.
        path_name (str): The path name extracted from the URL.
        db (Session, optional): The database session. Defaults to Depends(get_db).

    Returns:
        dict: The response data.

    Raises:
        JSONResponse: If the requested resource is not found.
        JSONResponse: If there is a bad request.
    """
    if not path_name:
        # Handle the root path
        return __handle_root(request)

    try:
        # get full path from request
        full_path = request.url.path
        if request.url.query:
            full_path += "?" + request.url.query

        result = None

        if REDIS:
            result = redis.get(full_path)

        if result:
            print("Cache hit")
            data = json.loads(result)
            main_entity = data.get("main_entity")
            main_query = data.get("main_query")
            top_value = data.get("top_value")
            is_count = data.get("is_count")
            count_queries_redis = data.get("count_queries_redis")
            as_of_value = data.get("as_of_value")
            from_to_value = data.get("from_to_value")
            single_result = data.get("single_result")

            if STREAMING_MODE == "sqlalchemy":
                async with db_sqlalchemy as session:
                    if is_count:
                        if COUNT_MODE == "LIMIT_ESTIMATE":
                            query_estimate = await session.execute(
                                text(count_queries_redis[0])
                            )
                            query_count = query_estimate.scalar()
                            if query_count == COUNT_ESTIMATE_THRESHOLD:
                                query_estimate = await session.execute(
                                    text(count_queries_redis[1]["query"]),
                                    count_queries_redis[1]["params"],
                                )
                                query_count = query_estimate.scalar()
                        elif COUNT_MODE == "ESTIMATE_LIMIT":
                            query_estimate = await session.execute(
                                text(count_queries_redis[0]["query"]),
                                count_queries_redis[0]["params"],
                            )
                            query_count = query_estimate.scalar()
                            if query_count < COUNT_ESTIMATE_THRESHOLD:
                                query_estimate = await session.execute(
                                    text(count_queries_redis[1])
                                )
                                query_count = query_estimate.scalar()
                        else:
                            query_estimate = await session.execute(
                                text(count_queries_redis[0])
                            )
                            query_count = query_estimate.scalar()

                    iot_count = (
                        '"@iot.count": ' + str(query_count) + ","
                        if is_count and not single_result
                        else ""
                    )

                    result = stream_results(
                        main_entity,
                        text(main_query),
                        session,
                        top_value,
                        iot_count,
                        as_of_value,
                        from_to_value,
                        single_result,
                        full_path,
                    )
            else:
                result = asyncpg_stream_results(
                    main_entity,
                    main_query,
                    pgpool,
                    top_value,
                    is_count,
                    count_queries_redis,
                    as_of_value,
                    from_to_value,
                    single_result,
                    full_path,
                )
        else:
            if REDIS:
                print("Cache miss")
            result = await sta2rest.STA2REST.convert_query(
                full_path, db_sqlalchemy
            )

        async def wrapped_result_generator(first_item):
            yield first_item
            async for item in result:
                yield item

        try:
            first_item = await anext(result)
            return StreamingResponse(
                wrapped_result_generator(first_item),
                media_type="application/json",
                status_code=status.HTTP_200_OK,
            )
        except TimeoutError:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "code": 503,
                    "type": "error",
                    "message": "Service Unavailable",
                },
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
    count_queries_redis,
    as_of_value,
    from_to_value,
    single_result,
    full_path,
):
    async with pgpool.acquire() as conn:
        async with conn.transaction():
            if is_count:
                if COUNT_MODE == "LIMIT_ESTIMATE":
                    query_count = await conn.fetchval(count_queries_redis[0])
                    if query_count == COUNT_ESTIMATE_THRESHOLD:
                        query_count = await conn.fetchval(
                            "SELECT sensorthings.count_estimate($1) AS estimated_count",
                            count_queries_redis[1]["params"][
                                "compiled_query_text"
                            ],
                        )
                elif COUNT_MODE == "ESTIMATE_LIMIT":
                    query_count = await conn.fetchval(
                        "SELECT sensorthings.count_estimate($1) AS estimated_count",
                        count_queries_redis[0]["params"][
                            "compiled_query_text"
                        ],
                    )
                    if query_count < COUNT_ESTIMATE_THRESHOLD:
                        query_count = await conn.fetchval(
                            count_queries_redis[1]
                        )
                else:
                    query_count = await conn.fetchval(count_queries_redis[0])

            iot_count = (
                '"@iot.count": ' + str(query_count) + ","
                if is_count and not single_result
                else ""
            )
            await conn.execute(f"DECLARE my_cursor CURSOR FOR {query}")

            start_json = ""
            is_first_partition = True
            has_rows = False

            while True:
                partition = await conn.fetch(
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
                    partition[0]["@iot.as_of"] = as_of_value

                processed_partition = [
                    ujson.loads(record["json"]) for record in partition
                ]

                partition_json = ujson.dumps(
                    processed_partition,
                    default=datetime.isoformat,
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

            await conn.execute("CLOSE my_cursor")
