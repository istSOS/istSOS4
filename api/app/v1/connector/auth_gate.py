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

"""
Connector authentication gate dependency.

Enforces the access control policy defined in IMPLEMENTATION_PLAN.md:
- AUTHORIZATION=0 or ANONYMOUS_VIEWER=1: Everything is public. No gate restriction.
- Strict mode (AUTHORIZATION=1, ANONYMOUS_VIEWER=0):
  - Any valid JWT passes (current_user is not None).
  - Closed-network override (network_id in CATALOG_CLOSED_NETWORKS):
    Requires authentication. Returns 404 (hidden) if unauthenticated.
  - Deep tier (deep_tier=True):
    Requires authentication. Returns 401 (login required) if unauthenticated.
  - Shallow tier (deep_tier=False):
    Gated by OPEN_CATALOG_METADATA. If 1, public. If 0, requires authentication (401).
"""

from typing import Optional
from fastapi import Depends, status
from fastapi.responses import JSONResponse

import app
from app.oauth import get_current_user_optional
import app.v1.connector.config as connector_config


def _login_required(detail: str = "Login required.") -> JSONResponse:
    """Return a 401 Unauthorized response with WWW-Authenticate header."""
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": detail},
        headers={"WWW-Authenticate": "Bearer"},
    )


def _hidden(detail: str = "Not found.") -> JSONResponse:
    """Return a 404 Not Found response matching api.py's _not_found shape."""
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "code": 404,
            "type": "error",
            "message": detail,
        },
    )


def make_gate(*, deep_tier: bool):
    """
    Factory creating a FastAPI dependency function for gating connector routes.

    :param deep_tier: True for deep tier routes (/dcat/orphan, /dcat/{network_id}),
                      False for shallow tier routes.
    """
    async def gate(
        network_id: Optional[int] = None,
        current_user: Optional[dict] = Depends(get_current_user_optional),
    ) -> Optional[JSONResponse]:
        if not getattr(app, "AUTHORIZATION", 0):
            return None
        if getattr(app, "ANONYMOUS_VIEWER", 0):
            return None

        # Strict mode
        if current_user is not None:
            return None

        closed_networks = getattr(connector_config, "CATALOG_CLOSED_NETWORKS", frozenset())
        if network_id is not None and network_id in closed_networks:
            return _hidden("Not found.")

        if deep_tier:
            return _login_required("Login required.")

        open_metadata = getattr(connector_config, "OPEN_CATALOG_METADATA", True)
        if not open_metadata:
            return _login_required("Login required.")

        return None

    return gate
