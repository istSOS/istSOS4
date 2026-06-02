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
from app.v1.endpoints.functions import set_role
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse, Response

from .functions import delete_entity, set_commit

v1 = APIRouter()

user = Header(default=None, include_in_schema=False)
message = Header(default=None, alias="commit-message", include_in_schema=False)

if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)

if VERSIONING or AUTHORIZATION:
    message = Header(alias="commit-message")


async def delete_staplus_entity(
    entity_name,
    entity_id,
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

                await set_commit(
                    connection,
                    commit_message,
                    current_user,
                    entity_name,
                    entity_id,
                )

                id_deleted = await delete_entity(
                    connection, entity_name, entity_id
                )

                if id_deleted is None:
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": f"{entity_name} with id {entity_id} not found",
                        },
                    )

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
    methods=["DELETE"],
    tags=["Parties"],
    summary="Delete a Party",
    description="Delete a Party by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_party(
    party_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await delete_staplus_entity(
        "Party", party_id, commit_message, current_user, pool
    )


@v1.api_route(
    "/Licenses({license_id})",
    methods=["DELETE"],
    tags=["Licenses"],
    summary="Delete a License",
    description="Delete a License by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_license(
    license_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await delete_staplus_entity(
        "License", license_id, commit_message, current_user, pool
    )


@v1.api_route(
    "/Campaigns({campaign_id})",
    methods=["DELETE"],
    tags=["Campaigns"],
    summary="Delete a Campaign",
    description="Delete a Campaign by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_campaign(
    campaign_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await delete_staplus_entity(
        "Campaign", campaign_id, commit_message, current_user, pool
    )


@v1.api_route(
    "/ObservationGroups({observation_group_id})",
    methods=["DELETE"],
    tags=["ObservationGroups"],
    summary="Delete an ObservationGroup",
    description="Delete an ObservationGroup by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_observation_group(
    observation_group_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await delete_staplus_entity(
        "ObservationGroup",
        observation_group_id,
        commit_message,
        current_user,
        pool,
    )


@v1.api_route(
    "/Relations({relation_id})",
    methods=["DELETE"],
    tags=["Relations"],
    summary="Delete a Relation",
    description="Delete a Relation by ID",
    status_code=status.HTTP_200_OK,
)
async def delete_relation(
    relation_id: int,
    commit_message=message,
    current_user=user,
    pool=Depends(get_pool_w) if POSTGRES_PORT_WRITE else Depends(get_pool),
):
    return await delete_staplus_entity(
        "Relation", relation_id, commit_message, current_user, pool
    )
