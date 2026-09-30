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

from app import (
    AUTHORIZATION,
    HOSTNAME,
    POSTGRES_PORT_WRITE,
    SUBPATH,
    VERSION,
)
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import require_json_content_type
from app.v1.endpoints.aggregation_job import (
    AGGREGATION_PROCEDURES,
    build_database_config,
    create_job,
    ensure_aggregation_available,
    fetch_aggregation_job,
    find_duplicate_job,
    lock_job_identity,
    raise_duplicate_job,
    require_administrator,
    resolve_datastreams,
    validate_create_payload,
    validate_database_options,
    validate_features_of_interest,
)
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

PAYLOAD_EXAMPLE = {
    "sourceDatastream": "Praw_COL",
    "targetDatastream": "P_COL",
    "bucketInterval": "10 minutes",
    "bucketTimezone": "Europe/Zurich",
    "aggregation": "SUM",
    "conversionFactor": 1.0,
    "bucketsToRecompute": 3,
    "boundaryMode": "right_closed",
    "resultQualityKey": "code",
    "resultQualityAllowed": [100, 200, 255],
    "resultQualityMin": 100,
    "resultQualityMax": 255,
    "availabilityDatastreams": ["Ta_COL"],
    "availabilityMaxAge": "5 minutes",
    "emptyBucketPolicy": "zero_when_available",
    "emptyBucketResultQuality": 210,
    "systemTimeIncremental": True,
    "systemTimeOverlap": "5 minutes",
    "featuresOfInterestId": None,
    "resultType": 0,
    "scheduleInterval": "10 minutes",
    "scheduleDelay": "3 minutes",
    "fixedSchedule": True,
    "scheduleTimezone": "Europe/Zurich",
    "enabled": True,
}


@v1.api_route(
    "/AggregationJobs",
    methods=["POST"],
    tags=["AggregationJobs"],
    summary="Create an aggregation job",
    description="Create and schedule a TimescaleDB Datastream aggregation job.",
    status_code=status.HTTP_201_CREATED,
)
async def create_aggregation_job(
    request: Request,
    payload: dict = Body(examples=[PAYLOAD_EXAMPLE]),
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    require_json_content_type(request)
    require_administrator(current_user)
    values = validate_create_payload(payload)
    procedure = AGGREGATION_PROCEDURES[values["bucketInterval"]]

    async with pool.acquire() as connection:
        async with connection.transaction():
            await ensure_aggregation_available(connection, procedure)
            await validate_database_options(connection, values)
            datastreams = await resolve_datastreams(connection, values)
            await validate_features_of_interest(
                connection, values.get("featuresOfInterestId")
            )

            await lock_job_identity(
                connection,
                procedure,
                datastreams[values["sourceDatastream"]]["id"],
                datastreams[values["targetDatastream"]]["id"],
            )

            duplicate_job_id = await find_duplicate_job(
                connection,
                procedure,
                values["sourceDatastream"],
                values["targetDatastream"],
                datastreams,
            )
            if duplicate_job_id is not None:
                raise_duplicate_job(duplicate_job_id, values)

            config = build_database_config(values, datastreams)
            job_id = await create_job(connection, values, config)
            job = await fetch_aggregation_job(connection, job_id)

    location = (
        f"{HOSTNAME}{SUBPATH}{VERSION}/AggregationJobs({job_id})"
    )
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content=job,
        headers={"location": location},
    )
