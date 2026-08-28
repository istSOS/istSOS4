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

from pydantic import BaseModel, field_validator

from app.rbac_roles import validate_rbac_role


class AdminApprovalRequest(BaseModel):
    """Request body for PATCH /Users/{target_user_id}/policy-approval.

    Fields
    ------
    assigned_role:   The application-layer RBAC role to grant to the target
                     user.  Must be one of the assignable roles defined in
                     ``VALID_RBAC_ROLES`` (e.g. viewer, editor, obs_manager,
                     sensor, custom).  The internal 'pending' state and
                     'administrator' may NOT be set through this endpoint.
    dataset_id:      Human-readable or URI identifier for the STAC dataset
                     to which access is being granted.  Forwarded to AuditLog.
    odrl_policy_id:  Identifier of the ODRL policy document that governs
                     access to the dataset.  Forwarded to AuditLog.
    """

    assigned_role: str
    dataset_id: str
    odrl_policy_id: str

    @field_validator("assigned_role")
    @classmethod
    def role_must_be_valid(cls, v: str) -> str:
        """Pass the value through validate_rbac_role.

        Raises ``ValueError`` (which Pydantic converts to a 422 response)
        if the role is not one of the permitted assignable roles.
        """
        return validate_rbac_role(v)
