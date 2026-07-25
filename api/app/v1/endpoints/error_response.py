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

"""Shared error-response helper for the STA write endpoints.

conformance refactor (P4): DRYs the error JSONResponse blocks duplicated across
the create/update/delete endpoints. Produces the EXACT body those blocks emitted
inline -- ``{"code": <status>, "type": "error", "message": <message>}`` -- so it
is strictly behaviour-preserving (same status code, byte-identical JSON body).
The auth/RBAC branches (401/403 InsufficientPrivilege) are intentionally left
inline and do NOT use this helper.
"""

from fastapi.responses import JSONResponse


def error_response(status_code: int, message) -> JSONResponse:
    """Build the canonical STA error response.

    The body ``code`` mirrors the HTTP ``status_code`` (they are always equal in
    the endpoints), ``type`` is the constant ``"error"``, and ``message`` is the
    caller-supplied controlled message (never raw driver/Postgres text).
    """
    return JSONResponse(
        status_code=status_code,
        content={"code": status_code, "type": "error", "message": message},
    )
