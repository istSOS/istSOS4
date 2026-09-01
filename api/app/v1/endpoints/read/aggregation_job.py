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
from app.v1.endpoints.aggregation_job import (
    ensure_aggregation_available,
    fetch_aggregation_job,
    fetch_aggregation_jobs,
    require_administrator,
)
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


@v1.api_route(
    "/AggregationJobs",
    methods=["GET"],
    tags=["AggregationJobs"],
    summary="Get aggregation jobs",
    description="Return all configured Datastream aggregation jobs.",
    status_code=status.HTTP_200_OK,
)
async def get_aggregation_jobs(
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    require_administrator(current_user)

    async with pool.acquire() as connection:
        await ensure_aggregation_available(connection)
        jobs = await fetch_aggregation_jobs(connection)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"value": jobs},
    )


@v1.api_route(
    "/AggregationJobs({job_id})",
    methods=["GET"],
    tags=["AggregationJobs"],
    summary="Get an aggregation job",
    description="Return one Datastream aggregation job.",
    status_code=status.HTTP_200_OK,
)
async def get_aggregation_job(
    job_id: int,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    require_administrator(current_user)

    async with pool.acquire() as connection:
        await ensure_aggregation_available(connection)
        job = await fetch_aggregation_job(connection, job_id)

    return JSONResponse(status_code=status.HTTP_200_OK, content=job)
