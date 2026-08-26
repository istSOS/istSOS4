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

"""Reusable OpenAPI ``responses=`` fragments for the auth/RBAC endpoints.

Purely declarative.  FastAPI merges these into the generated schema with
``deep_dict_update`` and never executes them, so nothing in this module can
change what an endpoint does at runtime.  (This is also why the endpoints
use ``responses=`` rather than ``response_model=``: every auth/RBAC handler
returns a ``JSONResponse`` directly, so ``response_model=`` would be inert
anyway, and ``responses=`` cannot touch the serialisation path at all.)

Each fragment names the body *shape* it documents, because this API emits
three different ones -- see app/models/error.py.  Pick the fragment that
matches how the endpoint genuinely fails; do not guess.  Every ``example``
below is copied from the actual ``raise``/``return`` site, not invented.

Usage::

    from app.v1.endpoints.openapi_responses import (
        ADMIN_ERRORS, NOT_FOUND_USER, INFRA_ERRORS, merge,
    )

    @v1.api_route(..., responses=merge(ADMIN_ERRORS, NOT_FOUND_USER, INFRA_ERRORS))

Beware: ``merge()`` is ``dict.update``, so a later fragment silently
replaces an earlier one that documents the same status code.  Where a
single endpoint has two distinct causes for one code (e.g. update/password
returns 403 both for "not an administrator" and for "not your own
account"), use the purpose-built combined fragment instead of merging two.
"""

from app.models.error import DetailError, MessageError, StaError
from app.models.token import TokenResponse

__all__ = [
    "merge",
    "response",
    # {"detail": ...}
    "UNAUTHORIZED",
    "LOGIN_FAILED",
    "BAD_REQUEST_TOKEN_FORMAT",
    "TOKEN_REVOKED",
    "FORBIDDEN_PENDING",
    "FORBIDDEN_ADMIN",
    "FORBIDDEN_ADMIN_OR_SELF",
    "NOT_FOUND_USER",
    "NOT_FOUND_PENDING_USER",
    "NOT_FOUND_PROVIDER",
    "CONFLICT_USERNAME",
    "CONFLICT_PROVIDER_USERNAME",
    "CONFLICT_LAST_ADMIN",
    "BAD_REQUEST_PENDING_ROLE",
    "BAD_REQUEST_REJECTED",
    # {"message": ...}
    "MSG_UNAUTHORIZED",
    "MSG_FORBIDDEN_DB",
    "MSG_NOT_FOUND",
    "OIDC_REGISTRATION_PENDING",
    "DB_UNAVAILABLE",
    "DB_TIMEOUT",
    "INTERNAL",
    # {"code", "type", "message"}
    "STA_BAD_REQUEST",
    "STA_NOT_FOUND",
    "STA_CONFLICT",
    "STA_FORBIDDEN",
    # token
    "TOKEN_OK",
    "BAD_REQUEST_OIDC_CALLBACK",
    "BAD_GATEWAY_PROVIDER_CLAIMS",
    # bundles
    "AUTH_ERRORS",
    "ADMIN_ERRORS",
    "INFRA_ERRORS",
]


def response(model, description: str, example: dict) -> dict:
    """Build one ``responses`` entry: a model reference plus a real example."""
    return {
        "model": model,
        "description": description,
        "content": {"application/json": {"example": example}},
    }


def merge(*groups: dict) -> dict:
    """Combine fragments into a single ``responses`` mapping.

    Later groups win on duplicate status codes -- see the module docstring's
    warning about endpoints with two causes for the same code.
    """
    out: dict = {}
    for group in groups:
        out.update(group)
    return out


# ---------------------------------------------------------------------------
# {"detail": ...}  -- raised as HTTPException
# ---------------------------------------------------------------------------

UNAUTHORIZED = {
    401: response(
        DetailError,
        "No bearer token was sent, or it is malformed, expired or revoked. "
        "Use the **Authorize** button, or POST /Login.",
        {"detail": "Could not validate credentials"},
    )
}

TOKEN_REVOKED = {
    401: response(
        DetailError,
        "The token was explicitly revoked by POST /Logout. Requires REDIS=1; "
        "without Redis a logged-out token stays valid until it expires.",
        {"detail": "Token has been revoked"},
    )
}

LOGIN_FAILED = {
    401: response(
        DetailError,
        "Unknown username, or the password does not match. Distinct from "
        "UNAUTHORIZED above -- this is a failed credential check, not a "
        "missing/invalid bearer token, since no token exists yet at login.",
        {"detail": "Incorrect username or password"},
    )
}

BAD_REQUEST_TOKEN_FORMAT = {
    400: response(
        DetailError,
        "The `Authorization` header is missing the `Bearer ` prefix, or the "
        "token itself fails to decode (malformed, wrong signature, or "
        "expired).",
        {"detail": "Invalid token"},
    )
}

FORBIDDEN_PENDING = {
    403: response(
        DetailError,
        "The token is valid, but the account is still in the `pending` "
        "waiting room and has no privileges. An administrator must approve "
        "it first via PATCH /Users/{id}/policy-approval.",
        {"detail": "Account pending admin activation"},
    )
}

FORBIDDEN_ADMIN = {
    403: response(
        DetailError,
        "Authenticated, but the caller is not an `administrator`.",
        {"detail": "Forbidden: Administrator access required"},
    )
}

FORBIDDEN_ADMIN_OR_SELF = {
    403: response(
        DetailError,
        "Two distinct causes share this code here: the caller is neither an "
        "administrator nor the owner of the target account.",
        {"detail": "You can only update your own password."},
    )
}

NOT_FOUND_USER = {
    404: response(
        DetailError,
        "No user exists with that id.",
        {"detail": "User with id=42 not found."},
    )
}

NOT_FOUND_PENDING_USER = {
    404: response(
        DetailError,
        "No user with that id, or the user is not in the `pending` state "
        "this action requires.",
        {"detail": "User not found or not in pending state"},
    )
}

CONFLICT_USERNAME = {
    409: response(
        DetailError,
        "The requested username is already taken by an active account. "
        "A previously *rejected* application is the exception: re-registering "
        "with the same username overwrites it and returns 201, not 409.",
        {"detail": "Username 'alice' is already taken."},
    )
}

CONFLICT_PROVIDER_USERNAME = {
    409: response(
        DetailError,
        "The identity provider's claimed username (its display name, "
        "preferred_username, or handle, depending on the provider) is "
        "already taken by an unrelated local or externally-linked account. "
        "Account linking is not implemented yet, so this is a dead end for "
        "the caller until an administrator intervenes.",
        {
            "detail": "Username 'alice' is already taken by an unrelated "
            "account. Account linking is not yet supported -- contact an "
            "administrator."
        },
    )
}

NOT_FOUND_PROVIDER = {
    404: response(
        DetailError,
        "The `{provider}` path segment doesn't match any of the five "
        "supported names, or this deployment hasn't configured that "
        "provider's `*_CLIENT_ID` / `*_CLIENT_SECRET`.",
        {
            "detail": "Unknown or unconfigured identity provider 'okta'. "
            "Enabled providers: google, github."
        },
    )
}

CONFLICT_LAST_ADMIN = {
    409: response(
        DetailError,
        "Refused: this is the last remaining administrator. Demoting them "
        "would leave the deployment with no one able to approve users or "
        "reassign roles.",
        {
            "detail": "Cannot demote the last administrator. "
            "Promote another user to administrator first."
        },
    )
}

BAD_REQUEST_PENDING_ROLE = {
    400: response(
        DetailError,
        "The target user is still `pending`. Role reassignment applies only "
        "to already-activated accounts.",
        {
            "detail": "Cannot reassign role for a pending user. "
            "Activate the account first via POST /Users/{id}/activate."
        },
    )
}

BAD_REQUEST_REJECTED = {
    400: response(
        DetailError,
        "The application was rejected. Rejection is a `status` transition "
        "that deliberately leaves `role` at `pending`, so this endpoint "
        "refuses it explicitly rather than silently re-approving. The "
        "applicant must re-apply via POST /Register.",
        {
            "detail": "This user's registration was rejected and cannot be "
            "approved directly. They must re-apply via POST /Register first."
        },
    )
}


BAD_REQUEST_OIDC_CALLBACK = {
    400: response(
        DetailError,
        "Two distinct causes share this code: the provider rejected or the "
        "user cancelled the consent screen (token exchange failed), or "
        "`/callback` was hit directly without a prior `/login` call, so no "
        "dataset/policy selection exists in the session.",
        {
            "detail": "No dataset/policy selection found for this session. "
            "Start the login at /auth/{provider}/login with dataset_id and "
            "odrl_policy_id, not at /callback directly."
        },
    )
}

BAD_GATEWAY_PROVIDER_CLAIMS = {
    502: response(
        DetailError,
        "The identity provider's response didn't contain the claims this "
        "API needs (e.g. no `sub`). Indicates a provider-side or "
        "configuration problem, not a caller error.",
        {"detail": "Identity provider returned an unexpected response."},
    )
}

# ---------------------------------------------------------------------------
# {"message": ...}  -- inline JSONResponse in the auth/RBAC handlers
# ---------------------------------------------------------------------------

MSG_UNAUTHORIZED = {
    401: response(
        MessageError,
        "The caller is not an administrator. Note this endpoint reports "
        "insufficient privileges as 401, not 403.",
        {"message": "Insufficient privileges."},
    )
}

MSG_FORBIDDEN_DB = {
    403: response(
        MessageError,
        "The caller's PostgreSQL role lacks a privilege this operation needs.",
        {"message": "Insufficient database privileges."},
    )
}

MSG_NOT_FOUND = {
    404: response(
        MessageError,
        "The requested user does not exist.",
        {"message": "User with id=42 not found."},
    )
}

OIDC_REGISTRATION_PENDING = {
    202: response(
        MessageError,
        "The handshake succeeded, but no token is issued. Two distinct "
        "cases share this code: a brand-new identity was just provisioned "
        "into the `pending` waiting room (example below), or an identity "
        "seen before is still awaiting an administrator's decision. "
        "External login never bypasses manual approval, the same as "
        "POST /Register.",
        {
            "message": "Registration submitted via google. An "
            "administrator must approve this account before you can log in."
        },
    )
}

DB_UNAVAILABLE = {
    503: response(
        MessageError,
        "The database is unreachable or has no free connections.",
        {"message": "Database temporarily unavailable."},
    )
}

DB_TIMEOUT = {
    504: response(
        MessageError,
        "The database statement exceeded its timeout.",
        {"message": "Database request timed out."},
    )
}

INTERNAL = {
    500: response(
        MessageError,
        "Unexpected server error. Details are logged server-side and "
        "deliberately never returned to the caller.",
        {"message": "Internal server error."},
    )
}


# ---------------------------------------------------------------------------
# {"code", "type", "message"}  -- error_response() via exception_handlers.py
# ---------------------------------------------------------------------------

STA_BAD_REQUEST = {
    400: response(
        StaError,
        "Malformed payload, or a referenced entity does not exist.",
        {"code": 400, "type": "error", "message": "Payload must be a dictionary."},
    )
}

STA_NOT_FOUND = {
    404: response(
        StaError,
        "The named entity does not exist.",
        {"code": 404, "type": "error", "message": "Not found."},
    )
}

STA_CONFLICT = {
    409: response(
        StaError,
        "The entity already exists.",
        {"code": 409, "type": "error", "message": "Entity already exists."},
    )
}

STA_FORBIDDEN = {
    403: response(
        StaError,
        "Insufficient PostgreSQL privileges for this operation.",
        {"code": 403, "type": "error", "message": "Insufficient privileges."},
    )
}


# ---------------------------------------------------------------------------
# Success bodies
# ---------------------------------------------------------------------------

TOKEN_OK = {
    200: response(
        TokenResponse,
        "Authentication succeeded. Note `expires_in` is an absolute Unix "
        "timestamp, not a remaining duration -- see the schema.",
        {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
            "token_type": "bearer",
            "expires_in": 1785569400,
        },
    )
}


# ---------------------------------------------------------------------------
# Bundles for the common combinations
# ---------------------------------------------------------------------------

#: Any endpoint behind Depends(get_current_user).
AUTH_ERRORS = merge(UNAUTHORIZED, FORBIDDEN_PENDING)

#: Administrator-only endpoints. The 403 here is the admin check, which
#: takes precedence over the pending check in practice.
ADMIN_ERRORS = merge(UNAUTHORIZED, FORBIDDEN_ADMIN)

#: The database/infrastructure tail every auth handler shares.
INFRA_ERRORS = merge(DB_UNAVAILABLE, DB_TIMEOUT, INTERNAL)
