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

from app import ANONYMOUS_VIEWER, AUTHORIZATION, REDIS
from app.db.asyncpg_db import get_pool
from app.db.redis_db import redis
from app.sta2rest import sta2rest
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from .query_parameters import CommonQueryParams, get_common_query_params
from .read import asyncpg_stream_results, wrapped_result_generator

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION and not ANONYMOUS_VIEWER:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/FeaturesOfInterest",
    methods=["GET"],
    tags=["FeaturesOfInterest"],
    summary="Get all features of interest",
    description="Returns all the features of interest provided by this api (subject to any parameters set)",
    status_code=status.HTTP_200_OK,
)
async def get_features_of_interest(
    request: Request,
    current_user=user,
    pool=Depends(get_pool),
    params: CommonQueryParams = Depends(get_common_query_params),
):
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
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )
