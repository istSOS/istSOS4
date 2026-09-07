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

from pydantic import BaseModel, field_validator

from app.validators import validate_password_strength


class PasswordUpdateRequest(BaseModel):
    """Request body for a local-password update.

    Attributes:
        current_password: The user's existing local istSOS credential, used to
            verify identity before the update is applied.
        new_password: The desired new password. Must satisfy the strength
            rules enforced by ``validate_password_strength`` — the same rule
            applied to a brand-new account's initial password at
            registration (see app.models.register_request), so the two
            entry points can never silently drift apart again.
    """

    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        return validate_password_strength(v)
