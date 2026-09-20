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

"""PATCH /Users/{id}/role — administrator-only role re-assignment endpoint.

Authorization
-------------
Only users with ``role == 'administrator'`` may call this endpoint.
Any other caller receives HTTP 403 before any database interaction occurs.

The endpoint is intentionally narrow: it does one thing (change a user's
application-level role) and delegates all DB logic + edge-case guards to
``role_crud.update_user_role``.
"""

import logging

from app.db.role_crud import update_user_role
from app.models.role import RoleUpdateRequest
from app.oauth import get_current_user
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.api_route(
    "/Users/{user_id}/role",
    methods=["PATCH"],
    tags=["Users"],
    summary="Re-assign a user's role (admin only)",
    description=(
        "Change the application-layer role for an existing, active user. "
        "Restricted to administrators. "
        "Pending users must be activated first. "
        "The last administrator cannot be demoted. "
        "Accepts: viewer, editor, obs_manager, sensor, custom."
    ),
    status_code=status.HTTP_204_NO_CONTENT,
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
            detail="Only administrators can reassign user roles.",
        )

    # Delegate all business logic and guards to the CRUD layer.
    await update_user_role(user_id=user_id, new_role=payload.role)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
