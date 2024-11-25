import json

from app import AUTHORIZATION, REDIS
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.sta2rest import sta2rest
from app.v1.endpoints.read.read import (
    asyncpg_stream_results,
    wrapped_result_generator,
)
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from .query_parameters import CommonQueryParams, get_common_query_params

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/Locations",
    methods=["GET"],
    tags=["Locations"],
    summary="Get all locations",
    description="Returns all the locations provided by this api (subject to any parameters set)",
    status_code=status.HTTP_200_OK,
)
async def get_locations(
    request: Request,
    current_user=user,
    pool=Depends(get_pool),
    params: CommonQueryParams = Depends(get_common_query_params),
):
    try:
        data = None

        if REDIS:
            result = redis.get(request.url.path)
            if result:
                data = json.loads(result)
                print("Cache hit")
            else:
                print("Cache miss")

        if not data:
            data = sta2rest.STA2REST.convert_query(request.url.path)

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
            request.url.path,
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
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )
