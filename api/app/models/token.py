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

"""Pydantic model describing the bearer-token body this API returns.

Documentation-only, like app/models/error.py: the handlers build this body
as a plain dict inside a ``JSONResponse``, so nothing serialises through
this model.  It exists so Swagger can show the shape.

Emitted by ``POST /Login``, ``POST /Refresh``, and the already-approved
branch of ``GET /auth/{provider}/callback``.
"""

from typing import Literal

from pydantic import BaseModel, Field


class TokenResponse(BaseModel):
    """A successful authentication response."""

    access_token: str = Field(
        description=(
            "Signed JWT. Send it on subsequent requests as "
            "`Authorization: Bearer <token>`. Carries `sub` (username) and "
            "`role`, but the role is re-read from the database on every "
            "request, so an administrator's role change takes effect "
            "immediately without the token being reissued."
        ),
        examples=["eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbiJ9..."],
    )
    token_type: Literal["bearer"] = Field(
        default="bearer",
        description="Always the string 'bearer'.",
    )
    expires_in: int = Field(
        description=(
            "Absolute expiry as a Unix epoch timestamp in seconds -- **not** "
            "a remaining-lifetime duration. Note this deviates from OAuth 2.0 "
            "(RFC 6749 section 5.1), where `expires_in` is defined as the "
            "number of seconds until expiry. Compute the remaining lifetime "
            "as `expires_in - now`. The token's own `exp` claim carries the "
            "same value."
        ),
        examples=[1785569400],
    )
