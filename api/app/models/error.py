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

"""Pydantic models describing the error bodies this API actually emits.

Documentation-only.  Nothing validates or serialises through these models --
they exist purely so the ``responses=`` fragments in
``app/v1/endpoints/openapi_responses.py`` can show a caller the body they
will really receive.

Three distinct shapes coexist, by history rather than design, and
``endpoints/error_response.py`` says so explicitly: the auth/RBAC branches
are *deliberately* left inline and do not route through the shared STA
helper.  Documenting a single invented ``ErrorResponse`` would therefore be
a fiction -- each endpoint must reference the shape it genuinely returns.

  * ``DetailError``   -- raised as ``HTTPException``; rendered by FastAPI's
    built-in handler.  e.g. oauth.py's 401/403, register_request.py's 409.
  * ``MessageError``  -- the inline ``JSONResponse`` blocks in the auth/RBAC
    handlers.  e.g. activate_user.py, admin_rejection.py, read/user.py.
  * ``StaError``      -- the canonical SensorThings body produced by
    ``error_response()`` via ``exception_handlers.py``.

A fourth shape, ``{"detail": [{"loc", "msg", "type"}]}``, is FastAPI's own
``RequestValidationError`` (HTTP 422).  It is generated automatically for
any route with a typed body or parameter, so it needs no model here.
"""

from typing import Literal

from pydantic import BaseModel, Field


class DetailError(BaseModel):
    """Body of an error raised as ``HTTPException(detail=...)``."""

    detail: str = Field(
        description="Human-readable reason for the failure.",
        examples=["Could not validate credentials"],
    )


class MessageError(BaseModel):
    """Body of an error returned as an inline ``JSONResponse``."""

    message: str = Field(
        description="Human-readable reason for the failure.",
        examples=["Database temporarily unavailable."],
    )


class StaError(BaseModel):
    """Canonical SensorThings error body (see endpoints/error_response.py)."""

    code: int = Field(
        description="Mirrors the HTTP status code.",
        examples=[400],
    )
    type: Literal["error"] = Field(
        default="error",
        description="Constant discriminator; always the string 'error'.",
    )
    message: str = Field(
        description=(
            "Controlled message. Never contains raw driver or PostgreSQL text."
        ),
        examples=["Payload must be a dictionary."],
    )
