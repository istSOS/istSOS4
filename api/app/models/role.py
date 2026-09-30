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

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.rbac_roles import ASSIGNABLE_ROLES, validate_rbac_role


class RoleUpdateRequest(BaseModel):
    """Request body for an administrator-initiated role / scope change.

    Both fields are optional; supply at least one.

    * ``role``    — new application-layer role. Accepted values: viewer,
                    editor, obs_manager, sensor, qc, custom.
    * ``dataset`` — new Network scope. A Network name sets the scope,
                    ``""`` clears it, omitting the field leaves it unchanged.

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

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"role": "editor", "dataset": "IDROLOGIA"}]
        }
    )

    role: str | None = Field(
        default=None,
        description=(
            "New application-layer role. `administrator` is bootstrap-only "
            "and is rejected here (seeded once at deploy time via the "
            "ISTSOS_ADMIN env var, never API-assignable); `pending` is an "
            "internal waiting-room state and is also rejected. Omit to "
            "leave the role unchanged."
        ),
        examples=["editor"],
        json_schema_extra={"enum": ASSIGNABLE_ROLES},
    )

    dataset: str | None = Field(
        default=None,
        description=(
            "New Network scope. A Network name limits the user to that "
            "network's Datastreams/Observations; `\"\"` clears any scope "
            "(full access, subject to role); omitting the field leaves the "
            "current scope unchanged. Must match an existing Network. "
            "Ignored when the deployment runs with NETWORK=0."
        ),
        examples=["IDROLOGIA"],
    )

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str | None) -> str | None:
        """Delegate to the shared RBAC validator when a role is supplied.

        Accepts:  viewer, editor, obs_manager, sensor, qc, custom.
        Rejects with ValueError (→ HTTP 422):
          - 'administrator' — bootstrap-only, never API-assignable.
          - 'pending'       — internal OIDC waiting-room state.
          - Any unknown string.
        """
        if v is None:
            return None
        try:
            return validate_rbac_role(v)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

    @model_validator(mode="after")
    def at_least_one_field(self):
        if self.role is None and self.dataset is None:
            raise ValueError(
                "Provide at least one of 'role' or 'dataset'."
            )
        return self
