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

from typing import Iterable

from app import AUTHORIZATION
from app.db.asyncpg_db import get_pool
from asyncpg.exceptions import InsufficientPrivilegeError
from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

v1 = APIRouter()


class _StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CRUDPermission(_StrictBaseModel):
    read: bool = False
    create: bool = False
    update: bool = False
    delete: bool = False


class ReadPermission(_StrictBaseModel):
    read: bool = False


class PermissionsPayload(_StrictBaseModel):
    users: CRUDPermission = CRUDPermission()
    policies: CRUDPermission = CRUDPermission()
    things: CRUDPermission = CRUDPermission()
    sensors: CRUDPermission = CRUDPermission()
    observations: CRUDPermission = CRUDPermission()
    datastreams: CRUDPermission = CRUDPermission()
    locations: CRUDPermission = CRUDPermission()
    observed_properties: CRUDPermission = CRUDPermission()
    features_of_interest: CRUDPermission = CRUDPermission()
    historical_locations: CRUDPermission = CRUDPermission()
    audit_log: ReadPermission = ReadPermission()
    perm_matrix: ReadPermission = ReadPermission()


class PermissionsResponse(_StrictBaseModel):
    username: str
    role: str
    permissions: PermissionsPayload


user = Header(default=None, include_in_schema=False)
if AUTHORIZATION:
    from app.oauth import get_current_user

    user = Depends(get_current_user)


_TABLE_TO_PERMISSION_KEY = {
    "thing": "things",
    "sensor": "sensors",
    "observation": "observations",
    "datastream": "datastreams",
    "location": "locations",
    "observedproperty": "observed_properties",
    "featureofinterest": "features_of_interest",
    "featuresofinterest": "features_of_interest",
    "historicallocation": "historical_locations",
}


def _normalize_table_name(table_name: str) -> str:
    return table_name.replace("_", "").replace('"', "").lower()


def _set_permissions_from_command(permission: CRUDPermission, command: str) -> None:
    cmd = command.upper()
    if cmd == "ALL":
        permission.read = True
        permission.create = True
        permission.update = True
        permission.delete = True
    elif cmd == "SELECT":
        permission.read = True
    elif cmd == "INSERT":
        permission.create = True
    elif cmd == "UPDATE":
        permission.update = True
    elif cmd == "DELETE":
        permission.delete = True


def _apply_admin_permissions(permissions: PermissionsPayload) -> None:
    permissions.users.read = True
    permissions.users.create = True
    permissions.users.update = True
    permissions.users.delete = True

    permissions.policies.read = True
    permissions.policies.create = True
    permissions.policies.update = True
    permissions.policies.delete = True

    permissions.things.read = True
    permissions.things.create = True
    permissions.things.update = True
    permissions.things.delete = True

    permissions.sensors.read = True
    permissions.sensors.create = True
    permissions.sensors.update = True
    permissions.sensors.delete = True

    permissions.observations.read = True
    permissions.observations.create = True
    permissions.observations.update = True
    permissions.observations.delete = True

    permissions.datastreams.read = True
    permissions.datastreams.create = True
    permissions.datastreams.update = True
    permissions.datastreams.delete = True

    permissions.locations.read = True
    permissions.locations.create = True
    permissions.locations.update = True
    permissions.locations.delete = True

    permissions.observed_properties.read = True
    permissions.observed_properties.create = True
    permissions.observed_properties.update = True
    permissions.observed_properties.delete = True

    permissions.features_of_interest.read = True
    permissions.features_of_interest.create = True
    permissions.features_of_interest.update = True
    permissions.features_of_interest.delete = True

    permissions.historical_locations.read = True
    permissions.historical_locations.create = True
    permissions.historical_locations.update = True
    permissions.historical_locations.delete = True

    permissions.audit_log.read = True
    permissions.perm_matrix.read = True


def _apply_policy_permissions(
    permissions: PermissionsPayload, policy_rows: Iterable
) -> None:
    for row in policy_rows:
        table_name = row["tablename"]
        command = row["cmd"]

        if table_name is None or command is None:
            continue

        permission_key = _TABLE_TO_PERMISSION_KEY.get(
            _normalize_table_name(str(table_name))
        )
        if permission_key is None:
            continue

        permission = getattr(permissions, permission_key)
        _set_permissions_from_command(permission, str(command))


def _extract_identity(current_user):
    if not isinstance(current_user, dict):
        return None, None

    username = current_user.get("username")
    role = current_user.get("role")

    if not isinstance(username, str) or not username:
        return None, None
    if not isinstance(role, str) or not role:
        return None, None

    return username, role


@v1.api_route(
    "/Permissions",
    methods=["GET"],
    tags=["Permissions"],
    summary="Get Permissions",
    description="Get capability flags for the current user",
    status_code=status.HTTP_200_OK,
    response_model=PermissionsResponse,
)
async def get_permissions(
    current_user=user,
    pool=Depends(get_pool),
):
    try:
        username, role = _extract_identity(current_user)
        if username is None or role is None:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"message": "Could not validate credentials"},
            )

        permissions = PermissionsPayload()

        if role == "administrator":
            _apply_admin_permissions(permissions)
        else:
            async with pool.acquire() as connection:
                query = """
                    SELECT tablename, cmd
                    FROM pg_policies
                    WHERE schemaname = 'sensorthings'
                      AND ($1 = ANY (roles)
                           OR $2 = ANY (roles)
                           OR 'public' = ANY (roles));
                """
                policy_rows = await connection.fetch(query, username, role)

            _apply_policy_permissions(permissions, policy_rows)

        return PermissionsResponse(
            username=username,
            role=role,
            permissions=permissions,
        )
    except InsufficientPrivilegeError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"message": "Insufficient privileges."},
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)},
        )
