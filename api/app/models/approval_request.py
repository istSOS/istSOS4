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

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.rbac_roles import ASSIGNABLE_ROLES, validate_rbac_role


class AdminApprovalRequest(BaseModel):
    """Request body for PATCH /Users/{target_user_id}/policy-approval.

    Fields
    ------
    assigned_role:   The application-layer RBAC role to grant to the target
                     user.  Must be one of the assignable roles defined in
                     ``VALID_RBAC_ROLES`` (viewer, editor, obs_manager,
                     sensor, qc, odrl_governed).  The internal 'pending'
                     state and 'administrator' may NOT be set through this
                     endpoint.
    dataset_id:      Human-readable or URI identifier for the STAC dataset
                     to which access is being granted.  Forwarded to
                     AuditLog, and -- when assigned_role is
                     'odrl_governed' -- used directly to build the
                     dataset-scoped row-level-security predicate.
    odrl_policy_id:  Identifier of the ODRL policy document that governs
                     access to the dataset.  Forwarded to AuditLog only;
                     not parsed or resolved by this API.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "assigned_role": "odrl_governed",
                    "dataset_id": "stac://alpine-snow-2024",
                    "odrl_policy_id": "odrl:policy:cc-by-nc",
                }
            ]
        }
    )

    assigned_role: str = Field(
        description=(
            "RBAC role to grant. `administrator` and `pending` are "
            "rejected -- see the model docstring."
        ),
        examples=["odrl_governed"],
        # See app/models/role.py for why this is json_schema_extra and not
        # a Literal/Enum type: the validator normalises with
        # .strip().lower() after Pydantic's own coercion, and an enum type
        # would reject non-canonical casing before that ever runs.
        json_schema_extra={"enum": ASSIGNABLE_ROLES},
    )
    dataset_id: str = Field(
        description=(
            "Dataset identifier. Forwarded to the audit log; also becomes "
            "the row-level-security predicate's value when assigned_role "
            "is `odrl_governed`."
        ),
        examples=["stac://alpine-snow-2024"],
    )
    odrl_policy_id: str = Field(
        description=(
            "ODRL policy document identifier. Recorded for audit purposes "
            "only -- not parsed or resolved by this API."
        ),
        examples=["odrl:policy:cc-by-nc"],
    )

    @field_validator("assigned_role")
    @classmethod
    def role_must_be_valid(cls, v: str) -> str:
        """Pass the value through validate_rbac_role.

        Raises ``ValueError`` (which Pydantic converts to a 422 response)
        if the role is not one of the permitted assignable roles.
        """
        return validate_rbac_role(v)


class ApprovalResponse(BaseModel):
    """Documentation-only: the body PATCH .../policy-approval returns on
    success. Built as a plain dict in the handler -- see app/models/error.py."""

    message: str = Field(examples=["User 'jdoe' (id=42) has been approved with role 'viewer'."])
    user_id: int = Field(examples=[42])
    granted_role: str = Field(examples=["odrl_governed"])
    dataset_id: str = Field(examples=["stac://alpine-snow-2024"])
    odrl_policy_id: str = Field(examples=["odrl:policy:cc-by-nc"])
