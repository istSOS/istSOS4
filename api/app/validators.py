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

"""Shared field-validation helpers used across multiple Pydantic models.

Kept separate from any single model module so that the same rule can be
imported by both the registration schema and the password-update schema
without duplicating (and risking drift between) the two.
"""


def validate_password_strength(password: str) -> str:
    """Enforce the minimum password strength requirements.

    Applied identically to a brand-new account's initial password
    (RestrictedRegistrationRequest.password) and to a subsequent password
    change (PasswordUpdateRequest.new_password) — the first password an
    account ever gets is not exempt from the same standard as every
    password after it.

    Rules:
      - At least 8 characters
      - At least 1 digit
      - At least 1 symbol (any non-alphanumeric character)

    Raises:
        ValueError: if any rule is violated (FastAPI maps this to HTTP 422).
    """
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters long.")
    if not any(c.isdigit() for c in password):
        raise ValueError("Password must contain at least one digit.")
    if not any(not c.isalnum() for c in password):
        raise ValueError("Password must contain at least one symbol.")
    return password
