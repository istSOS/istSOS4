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

"""Pydantic schema for the PATCH /Users/{id}/password endpoint."""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.validators import validate_password_strength


class PasswordUpdateRequest(BaseModel):
    """Request body for a local-password update.

    Attributes:
        current_password: The user's existing PostgreSQL password, used to
            verify identity before the update is applied.
        new_password: The desired new password. Must satisfy the strength
            rules enforced by ``validate_password_strength`` — the same rule
            applied to a brand-new account's initial password at
            registration (see app.models.register_request), so the two
            entry points can never silently drift apart again.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"current_password": "OldPass1!", "new_password": "NewPass2#"}]
        }
    )

    current_password: str = Field(
        description="The account's current password, verified before the update proceeds.",
    )
    # No min_length/pattern here on purpose: that would move a bad password
    # from validate_password_strength's specific 422 message to Pydantic's
    # generic one, changing what the caller sees for the same failure.
    new_password: str = Field(
        description=(
            "The desired new password. Must be at least 8 characters and "
            "contain at least one digit and at least one symbol "
            "(non-alphanumeric character). Enforced by the same rule "
            "applied to a brand-new account's initial password at "
            "POST /Register, so the two entry points can't drift apart."
        ),
        examples=["NewPass2#"],
    )

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        return validate_password_strength(v)
