import traceback

import ujson
from app import (
    DEBUG,
    HOSTNAME,
    REDIS,
    REDIS_CACHE_EXPIRATION,
    SUBPATH,
    VERSION,
)
from app.db.redis_db import redis
from app.db.sqlalchemy_db import get_db
from app.settings import serverSettings, tables
from app.sta2rest import sta2rest
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.exc import TimeoutError
from sqlalchemy.orm import Session

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
    request: Request, path_name: str, db: Session = Depends(get_db)
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
            try:
                result = redis.get(full_path)
            except Exception as e:
                print(e)
                result = None

        if not result:
            result = await sta2rest.STA2REST.convert_query(full_path, db)

            async def wrapped_result_generator(first_item):
                if REDIS:
                    accumulator = [first_item]
                yield first_item
                async for item in result:
                    if REDIS:
                        accumulator.append(item)
                    yield item

                if REDIS:
                    full_response = "".join(accumulator)
                    try:
                        redis.set(full_path, full_response)
                        redis.expire(full_path, REDIS_CACHE_EXPIRATION)
                    except Exception as e:
                        print(e)

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
        else:
            if REDIS:
                print("Cache hit")
                result = ujson.loads(result.decode("utf-8"))
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content=result,
                )

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
