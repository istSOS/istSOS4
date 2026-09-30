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
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from .query_parameters import CommonQueryParams, get_common_query_params
from .read import asyncpg_stream_results, stream_or_error

v1 = APIRouter()

user = Depends(lambda: None)

if AUTHORIZATION:
    from app.oauth import get_current_user, get_current_user_optional

    user = Depends(
        get_current_user_optional if ANONYMOUS_VIEWER else get_current_user
    )


@v1.api_route(
    "/Commits",
    methods=["GET"],
    tags=["Commits"],
    summary="Get all commits",
    description="Returns the commit history for all entities -- who "
    "changed what, and when -- across every dataset/Network. "
    "Administrator-only: a Commit carries no per-Network column of its "
    "own to scope on, so any non-admin role would see every network's "
    "edit history, not just its own. Requires VERSIONING=1 in the "
    "environment configuration.",
    status_code=status.HTTP_200_OK,
)
async def get_commits(
    request: Request,
    current_user=user,
    pool=Depends(get_pool),
    params: CommonQueryParams = Depends(get_common_query_params),
):
    try:
        # The Commit log has no RLS of its own -- the "user" group role's
        # blanket GRANT SELECT ON ALL TABLES (istsos_auth.sql) would let
        # ANY authenticated role read every commit, across every user and
        # every Network, once set_role() runs. Gate it here at the
        # application layer instead, the same way GET /Users and
        # GET /Policies are admin-only for the same reason (no clean
        # per-user / per-network row filter to write a policy against).
        # With ANONYMOUS_VIEWER=1 an anonymous caller arrives as None, which
        # must not be read as "no restriction" while authorization is on.
        if AUTHORIZATION and (
            current_user is None or current_user["role"] != "administrator"
        ):
            raise InsufficientPrivilegeError

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

        return await stream_or_error(result)
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
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": str(e),
            },
        )
