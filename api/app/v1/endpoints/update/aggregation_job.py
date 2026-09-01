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

from app import AUTHORIZATION, POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import require_json_content_type
from app.v1.endpoints.aggregation_job import (
    ensure_aggregation_available,
    fetch_aggregation_job,
    require_administrator,
    set_job_enabled,
    validate_patch_payload,
)
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

PAYLOAD_EXAMPLE = {"enabled": False}


@v1.api_route(
    "/AggregationJobs({job_id})",
    methods=["PATCH"],
    tags=["AggregationJobs"],
    summary="Activate or suspend an aggregation job",
    description="Change whether a Datastream aggregation job is scheduled.",
    status_code=status.HTTP_200_OK,
)
async def update_aggregation_job(
    request: Request,
    job_id: int,
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    require_json_content_type(request)
    require_administrator(current_user)
    values = validate_patch_payload(payload)

    async with pool.acquire() as connection:
        async with connection.transaction():
            await ensure_aggregation_available(connection)
            await set_job_enabled(connection, job_id, values["enabled"])
            job = await fetch_aggregation_job(connection, job_id)

    return JSONResponse(status_code=status.HTTP_200_OK, content=job)
