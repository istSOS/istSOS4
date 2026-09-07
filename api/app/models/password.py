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


class PasswordUpdateRequest(BaseModel):
    """Request body for a local-password update.

    Attributes:
        current_password: The user's existing PostgreSQL password, used to
            verify identity before the update is applied.
        new_password: The desired new password. Must satisfy the strength
            rules enforced by ``validate_new_password``.
    """

    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        """Enforce minimum password strength requirements.

        Rules:
          - At least 12 characters
          - At least 1 uppercase letter
          - At least 1 digit

        Raises:
            ValueError: if any rule is violated (FastAPI maps this to HTTP 422).
        """
        if len(v) < 12:
            raise ValueError("Password must be at least 12 characters long.")
        if not any(c.isupper() for c in v):
            raise ValueError(
                "Password must contain at least one uppercase letter."
            )
        if not any(c.isdigit() for c in v):
            raise ValueError(
                "Password must contain at least one digit."
            )
        return v
