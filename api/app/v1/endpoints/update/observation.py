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

import re
from datetime import datetime

from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.error_response import error_response
from app.v1.endpoints.exceptions import BadRequest
from app.v1.endpoints.functions import (
    set_role,
    update_datastream_observedArea,
    update_datastream_time_ranges,
)
from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import Response

from .functions import (
    get_observation_datastream_type,
    set_commit,
    update_observation_entity,
)
from .put import build_put_payload, parse_body, request_body_openapi_example

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(None, alias="commit-message")

PAYLOAD_EXAMPLE = {
    "phenomenonTime": "2015-03-03T00:00:00Z",
    "resultTime": "2015-03-03T00:00:00Z",
    "result": 3,
    "resultQuality": "100",
}

ALLOWED_KEYS = [
    "phenomenonTime",
    "result",
    "resultTime",
    "resultQuality",
    "validTime",
    "parameters",
    "Datastream",
    "FeatureOfInterest",
]


def parse_phenomenon_time(
    filter: str = Query(
        None,
        alias="$filter",
        description="Only `phenomenonTime eq <datetime>` is supported.",
    ),
):
    if filter is None:
        return None
    match = re.fullmatch(r"\s*phenomenonTime\s+eq\s+'?([^'\s]+)'?\s*", filter)
    if match is None:
        raise BadRequest(
            "Only $filter=phenomenonTime eq <datetime> is supported"
        )
    try:
        return datetime.fromisoformat(match.group(1).split("/")[0])
    except ValueError:
        raise BadRequest("Invalid phenomenonTime")


@v1.api_route(
    "/Observations({observation_id})",
    methods=["PATCH"],
    tags=["Observations"],
    summary="Update an Observation",
    description="Update an Observation",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def update_observation(
    observation_id: int,
    payload=Depends(parse_body),
    phenomenon_time=Depends(parse_phenomenon_time),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    if not observation_id:
        raise BadRequest("Observation ID not provided")

    async with pool.acquire() as connection:
        async with connection.transaction():
            if current_user is not None:
                await set_role(connection, current_user)

            found = await get_observation_datastream_type(
                connection, observation_id, phenomenon_time
            )
            if found is None:
                if current_user is not None:
                    await connection.execute("RESET ROLE;")
                return error_response(
                    status.HTTP_404_NOT_FOUND, "Observation not found."
                )

            if not payload:
                if current_user is not None:
                    await connection.execute("RESET ROLE;")
                return Response(status_code=status.HTTP_200_OK)

            validate_payload_keys(payload, ALLOWED_KEYS)

            commit_id = await set_commit(
                connection,
                commit_message,
                current_user,
            )
            if commit_id is not None:
                payload["commit_id"] = commit_id

            updated = await update_observation_entity(
                connection,
                observation_id,
                payload,
                phenomenon_time,
                observation_type=found[0],
            )

            await post_update_observation(
                connection, observation_id, payload, updated
            )

            if current_user is not None:
                await connection.execute("RESET ROLE;")

    return Response(status_code=status.HTTP_200_OK)


REQUIRED_PUT_KEYS = ["phenomenonTime", "resultTime", "result"]
OPTIONAL_PUT_KEYS = ["resultQuality", "validTime", "parameters"]


OBSERVATION_TIME_COLUMNS = {
    "phenomenonTimeStart",
    "phenomenonTimeEnd",
    "resultTime",
}


async def post_update_observation(
    connection, observation_id, payload, updated
):
    datastream_id = updated["datastream_id"] if updated else None

    if updated and OBSERVATION_TIME_COLUMNS & payload.keys():
        await update_datastream_time_ranges(
            connection,
            datastream_id,
            updated["phenomenonTimeStart"],
            updated["phenomenonTimeEnd"],
            updated["resultTime"],
            updated["resultTime"],
        )

    if payload.get("featuresofinterest_id") and datastream_id is not None:
        await update_datastream_observedArea(connection, datastream_id)


@v1.api_route(
    "/Observations({observation_id})",
    methods=["PUT"],
    tags=["Observations"],
    summary="Replace an Observation",
    description="Replace an Observation (full update)",
    status_code=status.HTTP_200_OK,
    openapi_extra=request_body_openapi_example(PAYLOAD_EXAMPLE),
)
async def replace_observation(
    observation_id: int,
    request: Request,
    phenomenon_time=Depends(parse_phenomenon_time),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    payload = build_put_payload(
        await parse_body(request), REQUIRED_PUT_KEYS, OPTIONAL_PUT_KEYS
    )
    return await update_observation(
        observation_id,
        payload,
        phenomenon_time,
        commit_message=commit_message,
        current_user=current_user,
        pool=pool,
    )
