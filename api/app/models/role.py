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

"""Pydantic schema for the PATCH /Users/{id}/role endpoint."""

from pydantic import BaseModel, field_validator

from app.rbac_roles import validate_rbac_role


class RoleUpdateRequest(BaseModel):
    """Request body for an administrator-initiated role re-assignment.

    Accepted values for ``role``: viewer, editor, obs_manager, sensor, custom.

    Why 'administrator' is intentionally blocked here
    -------------------------------------------------
    ``administrator`` is a bootstrap-only role in istSOS4. The initial admin
    account is seeded exclusively by the database initialisation script
    (``istsos_auth.sql``) using the ``ISTSOS_ADMIN`` environment variable at
    deploy time. It is deliberately absent from ``VALID_RBAC_ROLES`` so that
    the API can never be used to promote a standard user to administrator.
    This is a hard security boundary: infrastructure/DBA controls who holds
    administrative rights; the application API manages non-privileged roles
    only.

    Why a last-administrator demotion guard is still required in the CRUD layer
    ---------------------------------------------------------------------------
    Although the API cannot *promote* to administrator, it *can* receive a
    request to move a user whose current ``User.role`` is ``'administrator'``
    to a lower role. If that user is the only remaining administrator the
    system would be left in a permanently locked-out state. The CRUD layer
    therefore counts remaining admins and raises HTTP 409 before executing
    any mutation.
    """

    role: str

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        """Delegate to the shared RBAC validator.

        Accepts:  viewer, editor, obs_manager, sensor, custom.
        Rejects with ValueError (→ HTTP 422):
          - 'administrator' — bootstrap-only, never API-assignable.
          - 'pending'       — internal OIDC waiting-room state.
          - Any unknown string.
        """
        try:
            return validate_rbac_role(v)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
