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

"""Pydantic schema for PATCH /Users/{target_user_id}/reject.

Design decisions
----------------
* Rejection is a lifecycle transition (User.status), not an RBAC role
  assignment (User.role) — so this model carries no ``assigned_role``,
  unlike ``AdminApprovalRequest``.  The target user's role is left as
  'pending' by the endpoint; only status moves to 'rejected'.

* ``reason`` is optional free text, forwarded into the AuditLog payload
  for the ADMIN_REJECTION event.  Not looked up or validated beyond being
  a string.
"""

from pydantic import BaseModel, ConfigDict, Field


class RejectRequest(BaseModel):
    """Request body for PATCH /Users/{target_user_id}/reject.

    Fields
    ------
    reason: Optional free-text explanation for the rejection, recorded in
            the AuditLog payload.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"reason": "Affiliation could not be verified."}]
        }
    )

    reason: str | None = Field(
        default=None,
        description=(
            "Optional free text explaining the rejection, recorded in the "
            "ADMIN_REJECTION audit event's payload. Never shown to the "
            "applicant through this API."
        ),
        examples=["Affiliation could not be verified."],
    )


class RejectionResponse(BaseModel):
    """Documentation-only: the body PATCH .../reject returns on success."""

    message: str = Field(examples=["User 'jdoe' (id=42) has been rejected."])
    user_id: int = Field(examples=[42])
    status: str = Field(examples=["rejected"])
