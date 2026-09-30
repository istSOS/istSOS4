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

"""PATCH /Users/{id}/role — administrator-only role / scope change endpoint.

Authorization
-------------
Only users with ``role == 'administrator'`` may call this endpoint.
Any other caller receives HTTP 403 before any database interaction occurs.

The endpoint changes an active user's application role and/or their Network
scope (``User.dataset_id``); a user who wants either changed asks an
administrator, who is the sole gatekeeper. All DB logic + edge-case guards
live in ``role_crud.update_user_role``.
"""

import logging

from app.db.role_crud import update_user_role
from app.models.error import DetailError
from app.models.role import RoleUpdateRequest
from app.oauth import get_current_user
from app.v1.endpoints.openapi_responses import (
    ADMIN_ERRORS,
    BAD_REQUEST_PENDING_ROLE,
    CONFLICT_LAST_ADMIN,
    NOT_FOUND_USER,
    merge,
    response,
)
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{user_id}/role",
    methods=["PATCH"],
    tags=["Users"],
    summary="Change a user's role and/or Network scope (admin only)",
    description=(
        "Change the application-layer role and/or the Network scope "
        "(`User.dataset_id`) of an existing, active user. Send `role`, "
        "`dataset`, or both. `dataset: \"\"` clears the scope. "
        "Restricted to administrators. Pending users must be activated "
        "first. The last administrator cannot be demoted. "
        "Roles: viewer, editor, obs_manager, sensor, qc, custom."
    ),
    status_code=status.HTTP_204_NO_CONTENT,
    responses=merge(
        {
            204: {
                "description": (
                    "Role changed. No response body. Also returned "
                    "(with no database mutation) if the requested role "
                    "matches the current one -- a deliberate no-op guard."
                )
            },
            500: response(
                DetailError,
                "Unexpected database error during the role update.",
                {"detail": "Internal server error."},
            ),
        },
        ADMIN_ERRORS,
        NOT_FOUND_USER,
        BAD_REQUEST_PENDING_ROLE,
        CONFLICT_LAST_ADMIN,
    ),
)
async def patch_user_role(
    user_id: int,
    payload: RoleUpdateRequest,
    current_user=Depends(get_current_user),
):
    # ------------------------------------------------------------------
    # Authorization: administrators only.
    # ------------------------------------------------------------------
    if current_user.get("role") != "administrator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can change user roles or scope.",
        )

    # Delegate all business logic and guards to the CRUD layer.
    await update_user_role(
        user_id=user_id,
        new_role=payload.role,
        new_dataset=payload.dataset,
        actor_id=current_user.get("id"),
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
