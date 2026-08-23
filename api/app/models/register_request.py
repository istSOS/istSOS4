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

"""Pydantic schemas for POST /Register (restricted-access registration).

Design decisions
----------------
* ``ContactInfo`` is kept as a separate nested model so it serialises
  cleanly to a JSONB column via ``.model_dump()``.  Merging the flat
  ``explanation`` string into that dict at the DB layer (rather than here)
  keeps the model layer pure and transport-agnostic.

* All ``ContactInfo`` fields are ``Optional[str]`` (default ``None``) so
  a submitter need only supply the contact details they have available.

* ``RestrictedRegistrationRequest`` deliberately does *not* inherit any
  auth-aware base class — this endpoint is intentionally public (no
  ``Depends(get_current_user)``).  The pending role assigned in the DB
  ensures the new account has zero privileges until an admin approves it.
"""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.utils.utils import validate_username
from app.validators import validate_password_strength


class ContactInfo(BaseModel):
    """Optional structured contact details for a restricted-access applicant.

    All fields are optional; the applicant provides whatever is relevant.
    The entire model is stored as a single JSONB blob in
    ``sensorthings."User".contact``.
    """

    model_config = ConfigDict(
        # A partially-filled example, deliberately -- makes "every field
        # is optional" obvious at a glance rather than implying all six
        # are expected together.
        json_schema_extra={
            "examples": [{"company": "SUPSI", "telegram": "@jdoe"}]
        }
    )

    domain: Optional[str] = Field(default=None, description="Institutional or organizational domain, e.g. `supsi.ch`.")
    company: Optional[str] = Field(default=None, description="Employer or affiliated organization.")
    address: Optional[str] = Field(default=None, description="Postal or institutional address.")
    telephone: Optional[str] = Field(default=None, description="Contact phone number.")
    telegram: Optional[str] = Field(default=None, description="Telegram handle.")
    linkedin: Optional[str] = Field(default=None, description="LinkedIn profile URL.")


class RestrictedRegistrationRequest(BaseModel):
    """Request body for POST /Register.

    Fields
    ------
    username:       Desired login handle.  Uniqueness enforced at the DB level.
    password:       Plain-text password; hashed with bcrypt before storage.
    dataset_id:     Human-readable or URI identifier for the STAC dataset the
                    applicant wants access to.
    odrl_policy_id: Identifier of the ODRL policy document that governs access
                    to the requested dataset.
    explanation:    Free-text justification for the access request.  Stored
                    inside the ``contact`` JSONB blob alongside ContactInfo.
    contact_info:   Structured contact details for the applicant.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "username": "jdoe",
                    "password": "Str0ng!Pass",
                    "dataset_id": "stac://alpine-snow-2024",
                    "odrl_policy_id": "odrl:policy:cc-by-nc",
                    "explanation": "Requesting access for a climate research project.",
                    "contact_info": {"company": "SUPSI", "telegram": "@jdoe"},
                }
            ]
        }
    )

    username: str = Field(
        description="3-63 characters; letters, digits, and underscores only.",
        examples=["jdoe"],
    )
    password: str = Field(
        description=(
            "At least 8 characters, with at least one digit and at least "
            "one symbol (non-alphanumeric character). The same rule "
            "applied to every password change after this one -- see "
            "PATCH /Users/{id}/password."
        ),
        examples=["Str0ng!Pass"],
    )
    dataset_id: str = Field(
        description=(
            "Dataset the applicant is requesting access to. Persisted on "
            "the User row (not just the audit log) so an administrator "
            "reviewing the pending queue can see it, and forwarded to the "
            "RESTRICTED_REQUEST audit event."
        ),
        examples=["stac://alpine-snow-2024"],
    )
    odrl_policy_id: str = Field(
        description=(
            "ODRL policy document identifier governing that dataset. Same "
            "persistence as dataset_id; not parsed or resolved by this API."
        ),
        examples=["odrl:policy:cc-by-nc"],
    )
    explanation: str = Field(
        description="Free-text justification, merged into the `contact` JSONB blob alongside contact_info.",
        examples=["Requesting access for a climate research project."],
    )
    contact_info: ContactInfo = Field(
        description="Structured contact details -- every field is individually optional.",
    )

    @field_validator("username")
    @classmethod
    def username_must_be_valid(cls, v: str) -> str:
        """Enforce the same format rule as admin-created users.

        Self-registration previously accepted any string at all — the
        admin-created-user path (create/user.py) already enforces this
        via validate_username(); reusing it here rather than duplicating
        the pattern keeps the two entry points from drifting apart.
        """
        if not validate_username(v):
            raise ValueError(
                "Username must be 3-63 characters long and contain only "
                "letters, digits, and underscores."
            )
        return v

    @field_validator("password")
    @classmethod
    def password_must_be_strong(cls, v: str) -> str:
        """Enforce the same strength rule as password updates.

        A brand-new account's initial password is not exempt from the
        standard applied to every password after it — see
        app.validators.validate_password_strength.
        """
        return validate_password_strength(v)


class RegisterResponse(BaseModel):
    """Documentation-only: the body POST /Register returns on success.

    Built as a plain dict in the handler, not serialised through this
    model -- see app/models/error.py for why (JSONResponse, not
    response_model=).
    """

    id: int = Field(description="Primary key of the newly-created pending user.", examples=[42])
    status: str = Field(description="Always 'pending' immediately after registration.", examples=["pending"])
    message: str = Field(examples=["Registration submitted. Your account (id=42) is pending administrator approval."])
