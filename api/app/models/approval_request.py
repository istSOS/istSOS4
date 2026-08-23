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

"""Pydantic schemas for PATCH /Users/{target_user_id}/policy-approval.

Design decisions
----------------
* ``assigned_role`` is validated via ``validate_rbac_role`` at model
  instantiation time (field_validator), so the endpoint handler never
  receives an unknown or internal role (e.g. 'pending', 'administrator').

* ``dataset_id`` and ``odrl_policy_id`` are plain strings — they are
  forwarded verbatim into the AuditLog and are not looked up in the DB
  by this model; validation of their *existence* happens at the DB layer.

* The model intentionally carries no auth context; the endpoint handler
  enforces the administrator check via Depends(get_current_user).
"""

from pydantic import BaseModel, Field, field_validator

from app.rbac_roles import validate_rbac_role


class AdminApprovalRequest(BaseModel):
    """Request body for PATCH /Users/{target_user_id}/policy-approval.

    Fields
    ------
    assigned_role:   The application-layer RBAC role to grant to the target
                     user.  Optional -- if omitted, the endpoint falls back
                     to the ``requested_role`` the applicant stated at
                     registration (see register_request.py). Supplying a
                     value here always overrides that default; the
                     administrator is the final gatekeeper either way. Must
                     be one of the assignable roles defined in
                     ``VALID_RBAC_ROLES`` (viewer, editor, obs_manager,
                     sensor, qc, odrl_governed) if given.  The internal
                     'pending' state and 'administrator' may NOT be set
                     through this endpoint.
    dataset_id:      Human-readable or URI identifier for the STAC dataset
                     to which access is being granted.  Forwarded to AuditLog.
    odrl_policy_id:  Identifier of the ODRL policy document that governs
                     access to the dataset.  Forwarded to AuditLog.
    """

    assigned_role: str | None = None
    dataset_id: str
    odrl_policy_id: str

    @field_validator("assigned_role")
    @classmethod
    def role_must_be_valid(cls, v: str | None) -> str | None:
        """Pass the value through validate_rbac_role, unless omitted.

        None means "use the applicant's requested_role" -- resolved by the
        endpoint handler, which has the DB row this model doesn't. Only a
        supplied value is validated here.

        Raises ``ValueError`` (which Pydantic converts to a 422 response)
        if the role is not one of the permitted assignable roles.
        """
        if v is None:
            return None
        return validate_rbac_role(v)


class ApprovalResponse(BaseModel):
    """Documentation-only: the body PATCH .../policy-approval returns on
    success. Built as a plain dict in the handler -- see app/models/error.py."""

    message: str = Field(examples=["User 'jdoe' (id=42) has been approved with role 'viewer'."])
    user_id: int = Field(examples=[42])
    granted_role: str = Field(examples=["odrl_governed"])
    dataset_id: str = Field(examples=["stac://alpine-snow-2024"])
    odrl_policy_id: str = Field(examples=["odrl:policy:cc-by-nc"])
