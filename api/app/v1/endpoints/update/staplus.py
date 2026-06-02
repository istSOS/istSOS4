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
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Body, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .functions import (
    check_id_exists,
    set_commit,
    update_campaign_entity,
    update_license_entity,
    update_observation_group_entity,
    update_party_entity,
    update_relation_entity,
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
}

LICENSE_EXAMPLE = {
    "name": "CC BY 4.0",
    "description": "Creative Commons Attribution 4.0 International.",
    "attributionText": "Hydrology Team",
}

CAMPAIGN_EXAMPLE = {
    "name": "Spring monitoring campaign",
    "description": "Seasonal observation campaign.",
    "termsOfUse": "Campaign data is shared under the linked license.",
}

OBSERVATION_GROUP_EXAMPLE = {
    "name": "Validated spring observations",
    "description": "Grouped observations for the campaign.",
}

RELATION_EXAMPLE = {
    "role": "http://www.w3.org/2002/07/owl#sameAs",
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


async def update_staplus_entity(
    entity_name,
    entity_id,
    payload,
    allowed_keys,
    update_func,
    commit_message,
    current_user,
    pool,
):
    try:
        if not entity_id:
            raise Exception(f"{entity_name} ID not provided")

        async with pool.acquire() as connection:
            async with connection.transaction():
                if current_user is not None:
                    await set_role(connection, current_user)

                if not await check_id_exists(
                    connection, entity_name, entity_id
                ):
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")

                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": f"{entity_name} not found.",
                        },
                    )

                if not payload:
                    if current_user is not None:
                        await connection.execute("RESET ROLE;")
                    return Response(status_code=status.HTTP_200_OK)

                validate_payload_keys(payload, allowed_keys)

                commit_id = await set_commit(
                    connection, commit_message, current_user
                )
                if commit_id is not None:
                    payload["commit_id"] = commit_id

                await update_func(connection, entity_id, payload)

                if current_user is not None:
                    await connection.execute("RESET ROLE;")

        return Response(status_code=status.HTTP_200_OK)
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
            content={"code": 400, "type": "error", "message": str(e)},
        )


@v1.api_route(
    "/Parties({party_id})",
    methods=["PATCH"],
    tags=["Parties"],
    summary="Update a Party",
    description="Update a Party",
    status_code=status.HTTP_200_OK,
)
async def update_party(
    party_id: int,
    payload: dict = Body(example=PARTY_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await update_staplus_entity(
        "Party",
        party_id,
        payload,
        PARTY_KEYS,
        update_party_entity,
        commit_message,
        current_user,
        pool,
    )


@v1.api_route(
    "/Licenses({license_id})",
    methods=["PATCH"],
    tags=["Licenses"],
    summary="Update a License",
    description="Update a License",
    status_code=status.HTTP_200_OK,
)
async def update_license(
    license_id: int,
    payload: dict = Body(example=LICENSE_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await update_staplus_entity(
        "License",
        license_id,
        payload,
        LICENSE_KEYS,
        update_license_entity,
        commit_message,
        current_user,
        pool,
    )


@v1.api_route(
    "/Campaigns({campaign_id})",
    methods=["PATCH"],
    tags=["Campaigns"],
    summary="Update a Campaign",
    description="Update a Campaign",
    status_code=status.HTTP_200_OK,
)
async def update_campaign(
    campaign_id: int,
    payload: dict = Body(example=CAMPAIGN_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await update_staplus_entity(
        "Campaign",
        campaign_id,
        payload,
        CAMPAIGN_KEYS,
        update_campaign_entity,
        commit_message,
        current_user,
        pool,
    )


@v1.api_route(
    "/ObservationGroups({observation_group_id})",
    methods=["PATCH"],
    tags=["ObservationGroups"],
    summary="Update an ObservationGroup",
    description="Update an ObservationGroup",
    status_code=status.HTTP_200_OK,
)
async def update_observation_group(
    observation_group_id: int,
    payload: dict = Body(example=OBSERVATION_GROUP_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await update_staplus_entity(
        "ObservationGroup",
        observation_group_id,
        payload,
        OBSERVATION_GROUP_KEYS,
        update_observation_group_entity,
        commit_message,
        current_user,
        pool,
    )


@v1.api_route(
    "/Relations({relation_id})",
    methods=["PATCH"],
    tags=["Relations"],
    summary="Update a Relation",
    description="Update a Relation",
    status_code=status.HTTP_200_OK,
)
async def update_relation(
    relation_id: int,
    payload: dict = Body(example=RELATION_EXAMPLE),
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await update_staplus_entity(
        "Relation",
        relation_id,
        payload,
        RELATION_KEYS,
        update_relation_entity,
        commit_message,
        current_user,
        pool,
    )
