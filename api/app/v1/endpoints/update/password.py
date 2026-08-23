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

"""PATCH /Users/{id}/password — local-password update endpoint.

Authorization rules
-------------------
* A user may update their **own** password (current_user["id"] == id).
* An ``administrator`` may update **any** user's password.
* All other combinations → 403 Forbidden.
"""

import logging

from app.db.password_crud import update_local_password
from app.models.error import DetailError
from app.models.password import PasswordUpdateRequest
from app.oauth import get_current_user
from app.v1.endpoints.openapi_responses import (
    FORBIDDEN_ADMIN_OR_SELF,
    NOT_FOUND_USER,
    merge,
    response,
)
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{user_id}/password",
    methods=["PATCH"],
    tags=["Users"],
    summary="Update a local user's password",
    description=(
        "Change the PostgreSQL password for a local (non-OIDC) user. "
        "External identities (auth_provider IS NOT NULL) are blocked with "
        "HTTP 400. Requires the caller to supply their current password for "
        "verification. Admins may update any user's password; regular users "
        "may only update their own."
    ),
    status_code=status.HTTP_204_NO_CONTENT,
    responses=merge(
        {
            204: {"description": "Password changed. No response body."},
            # Two distinct causes share 401: no/invalid/expired bearer
            # token (from the auth dependency), or a valid token whose
            # current_password doesn't match (from update_local_password).
            401: response(
                DetailError,
                "Either no valid bearer token was sent, or the supplied "
                "current_password is wrong.",
                {"detail": "Current password is incorrect."},
            ),
            400: response(
                DetailError,
                "The target account is an external identity (has no local "
                "PostgreSQL password to change).",
                {"detail": "External identities cannot update passwords locally."},
            ),
            500: response(
                DetailError,
                "The password hash update itself failed unexpectedly.",
                {"detail": "Failed to update password."},
            ),
        },
        FORBIDDEN_ADMIN_OR_SELF,
        NOT_FOUND_USER,
    ),
)
async def update_password(
    user_id: int,
    payload: PasswordUpdateRequest,
    current_user=Depends(get_current_user),
):
    # ------------------------------------------------------------------
    # Authorization: own account OR administrator
    # ------------------------------------------------------------------
    is_admin = current_user["role"] == "administrator"
    is_own   = current_user["id"] == user_id

    if not (is_admin or is_own):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only update your own password.",
        )

    # Delegate all DB logic + edge-case guards to the CRUD layer
    await update_local_password(
        user_id=user_id,
        current_password=payload.current_password,
        new_password=payload.new_password,
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
