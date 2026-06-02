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

from app import AUTHORIZATION, POSTGRES_PORT_WRITE, VERSIONING
from app.db.asyncpg_db import get_pool, get_pool_w
from app.utils.utils import validate_payload_keys
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError, UniqueViolationError
from fastapi import APIRouter, Body, Depends, Header, Request, status
from fastapi.responses import JSONResponse, Response

from .functions import (
    insert_campaign_entity,
    insert_license_entity,
    insert_observation_group_entity,
    insert_party_entity,
    insert_relation_entity,
    set_commit,
)

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")

PARTY_EXAMPLE = {
    "displayName": "Hydrology Team",
    "description": "Data producing organization.",
    "authId": "hydrology-team",
    "role": "institutional",
}

LICENSE_EXAMPLE = {
    "name": "CC BY 4.0",
    "description": "Creative Commons Attribution 4.0 International.",
    "definition": "https://creativecommons.org/licenses/by/4.0/",
    "logo": "https://creativecommons.org/images/deed/cc_icon_white_x2.png",
    "attributionText": "Hydrology Team",
}

CAMPAIGN_EXAMPLE = {
    "name": "Spring monitoring campaign",
    "description": "Seasonal observation campaign.",
    "termsOfUse": "Campaign data is shared under the linked license.",
    "Party": {"@iot.id": 1},
    "License": {"@iot.id": 1},
}

OBSERVATION_GROUP_EXAMPLE = {
    "name": "Validated spring observations",
    "description": "Grouped observations for the campaign.",
    "Campaigns": [{"@iot.id": 1}],
}

RELATION_EXAMPLE = {
    "role": "http://www.w3.org/2002/07/owl#sameAs",
    "ObservationGroups": [{"@iot.id": 1}],
    "Subject": {"@iot.id": 1},
    "Object": {"@iot.id": 2},
}

PARTY_KEYS = [
    "displayName",
    "description",
    "authId",
    "role",
    "Datastreams",
    "Things",
    "Campaigns",
    "ObservationGroups",
]

LICENSE_KEYS = [
    "name",
    "description",
    "definition",
    "logo",
    "attributionText",
    "Datastreams",
    "Campaigns",
    "ObservationGroups",
]

CAMPAIGN_KEYS = [
    "name",
    "description",
    "classification",
    "termsOfUse",
    "privacyPolicy",
    "url",
    "creationTime",
    "startTime",
    "endTime",
    "Datastreams",
    "Party",
    "License",
    "ObservationGroups",
]

OBSERVATION_GROUP_KEYS = [
    "name",
    "description",
    "purpose",
    "creationTime",
    "endTime",
    "termsOfUse",
    "privacyPolicy",
    "dataQuality",
    "properties",
    "Campaigns",
    "Party",
    "License",
    "Observations",
    "Relations",
]

RELATION_KEYS = [
    "role",
    "description",
    "properties",
    "ObservationGroups",
    "Subject",
    "Object",
    "externalResource",
]


async def create_staplus_entity(
    request,
    payload,
    allowed_keys,
    insert_func,
    commit_message,
    current_user,
    pool,
    duplicate_message,
):
    try:
        if (
            "content-type" not in request.headers
            or request.headers["content-type"] != "application/json"
        ):
            raise Exception("Only content-type application/json is supported.")

        validate_payload_keys(payload, allowed_keys)

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )

                _, header = await insert_func(
                    connection, payload, commit_id=commit_id
                )

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(
            status_code=status.HTTP_201_CREATED,
            headers={"location": header},
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "code": 401,
                "type": "error",
                "message": "Insufficient privileges.",
            },
        )
    except UniqueViolationError:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "code": 409,
                "type": "error",
                "message": duplicate_message,
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


@v1.api_route(
    "/Parties",
    methods=["POST"],
    tags=["Parties"],
    summary="Create a new Party",
    description="Create a new Party entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_party(
    request: Request,
    payload: dict = Body(example=PARTY_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await create_staplus_entity(
        request,
        payload,
        PARTY_KEYS,
        insert_party_entity,
        commit_message,
        current_user,
        pool,
        "Party already exists.",
    )


@v1.api_route(
    "/Licenses",
    methods=["POST"],
    tags=["Licenses"],
    summary="Create a new License",
    description="Create a new License entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_license(
    request: Request,
    payload: dict = Body(example=LICENSE_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await create_staplus_entity(
        request,
        payload,
        LICENSE_KEYS,
        insert_license_entity,
        commit_message,
        current_user,
        pool,
        "License already exists.",
    )


@v1.api_route(
    "/Campaigns",
    methods=["POST"],
    tags=["Campaigns"],
    summary="Create a new Campaign",
    description="Create a new Campaign entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_campaign(
    request: Request,
    payload: dict = Body(example=CAMPAIGN_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await create_staplus_entity(
        request,
        payload,
        CAMPAIGN_KEYS,
        insert_campaign_entity,
        commit_message,
        current_user,
        pool,
        "Campaign already exists.",
    )


@v1.api_route(
    "/ObservationGroups",
    methods=["POST"],
    tags=["ObservationGroups"],
    summary="Create a new ObservationGroup",
    description="Create a new ObservationGroup entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_observation_group(
    request: Request,
    payload: dict = Body(example=OBSERVATION_GROUP_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await create_staplus_entity(
        request,
        payload,
        OBSERVATION_GROUP_KEYS,
        insert_observation_group_entity,
        commit_message,
        current_user,
        pool,
        "ObservationGroup already exists.",
    )


@v1.api_route(
    "/Relations",
    methods=["POST"],
    tags=["Relations"],
    summary="Create a new Relation",
    description="Create a new Relation entity.",
    status_code=status.HTTP_201_CREATED,
)
async def create_relation(
    request: Request,
    payload: dict = Body(example=RELATION_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await create_staplus_entity(
        request,
        payload,
        RELATION_KEYS,
        insert_relation_entity,
        commit_message,
        current_user,
        pool,
        "Relation already exists.",
    )
