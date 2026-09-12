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

"""POST /Register — Public self-registration for restricted dataset access.

Flow
----
1.  Hash the incoming plain-text password with bcrypt via asyncio.to_thread
    (passlib.hash is CPU-bound and would block the event loop).
2.  Build a merged contact JSONB blob:
    ``contact_info`` dict  +  ``{"explanation": request.explanation}``.
3.  Open a connection from the write pool inside a single DB transaction:
    a.  INSERT a new row into ``sensorthings."User"`` with role='pending'
        and status='active'.  RETURNING id to capture the auto-assigned PK.
    b.  UPDATE the new row's ``uri`` column to ``/Users(<id>)``.
    c.  Call ``log_audit_event`` (on the same connection / same transaction)
        with action_type='RESTRICTED_REQUEST' so the registration is
        atomically recorded in the AuditLog.
4.  Return HTTP 201 with the newly assigned user id.

Architecture notes
------------------
* The endpoint is intentionally **public** (no ``Depends(get_current_user)``).
  Any unauthenticated user may submit a registration request; the resulting
  account is locked in the ``pending`` role with zero operational privileges
  until an administrator explicitly activates it via POST /Users/{id}/activate.

* Password hashing is offloaded to ``asyncio.to_thread`` because
  ``passlib.CryptContext.hash`` is a synchronous, CPU-intensive bcrypt
  operation.  Calling it directly in an async handler would stall the
  entire Uvicorn event loop.

* The JSONB contact column is populated via ``json.dumps`` + ``$N::jsonb``
  cast, following the pattern established in ``audit_crud.py`` and
  ``oidc_user_crud.py``.  asyncpg does not auto-serialise Python dicts to
  PostgreSQL JSONB.

* All three DB writes (INSERT User, UPDATE uri, INSERT AuditLog) share one
  transaction via ``conn.transaction()``.  If the audit INSERT fails the
  user row is also rolled back, preventing orphaned records with no paper
  trail.
"""

import asyncio
import json
import logging

from app import POSTGRES_PORT_WRITE
from app.db.asyncpg_db import get_pool, get_pool_w
from app.db.audit_crud import AUDIT_ACTION_RESTRICTED_REQUEST, log_audit_event
from app.db.password_crud import pwd_context
from app.models.register_request import RestrictedRegistrationRequest
from asyncpg.exceptions import (
    PostgresConnectionError,
    QueryCanceledError,
    TooManyConnectionsError,
    UniqueViolationError,
)
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

v1 = APIRouter()
logger = logging.getLogger(__name__)


@v1.post(
    "/Register",
    tags=["Users"],
    summary="Submit a restricted-access registration request",
    description=(
        "Public endpoint. Creates a new user account in the 'pending' role "
        "and records a RESTRICTED_REQUEST audit event. "
        "No authentication required. "
        "The account grants zero operational access until an administrator "
        "activates it via POST /Users/{id}/activate."
    ),
    status_code=status.HTTP_201_CREATED,
)
async def register_request(request: RestrictedRegistrationRequest):
    """Handle a restricted-access self-registration.

    Args:
        request: Validated ``RestrictedRegistrationRequest`` body, parsed
                 and type-checked by FastAPI / Pydantic before this handler
                 is called.

    Returns:
        JSONResponse 201 containing ``{"id": <new_user_id>, "status": "pending"}``.

    Raises:
        HTTP 409: username is already taken (UniqueViolationError from DB).
        HTTP 503: database temporarily unavailable.
        HTTP 504: database request timed out.
        HTTP 500: any other unexpected error.
    """
    # ------------------------------------------------------------------
    # 1. Hash the password off the event loop — bcrypt is CPU-bound.
    # ------------------------------------------------------------------
    hashed_password: str = await asyncio.to_thread(
        pwd_context.hash, request.password
    )

    # ------------------------------------------------------------------
    # 2. Build the merged contact JSONB blob.
    #    Start from the ContactInfo model dict, then inject explanation so
    #    everything is stored in one JSON document.
    # ------------------------------------------------------------------
    contact_dict: dict = request.contact_info.model_dump()
    contact_dict["explanation"] = request.explanation
    contact_json: str = json.dumps(contact_dict)

    # ------------------------------------------------------------------
    # 3. Acquire a write connection and execute all mutations atomically.
    # ------------------------------------------------------------------
    try:
        write_pool = await get_pool_w() if POSTGRES_PORT_WRITE else await get_pool()

        async with write_pool.acquire() as conn:
            async with conn.transaction():

                # 3a. INSERT the new User row.
                #     role='pending'  → zero operational privileges.
                #     status='active' → account exists and can be found by admin.
                #     The RETURNING clause gives us the auto-assigned PK.
                row = await conn.fetchrow(
                    """
                    INSERT INTO sensorthings."User"
                        (username, password, role, status, contact)
                    VALUES
                        ($1, $2, 'pending', 'active', $3::jsonb)
                    RETURNING id
                    """,
                    request.username,
                    hashed_password,
                    contact_json,
                )
                new_user_id: int = row["id"]

                # 3b. Backfill the URI column now that we have the PK.
                #     Matches the pattern used in oidc_user_crud.py.
                await conn.execute(
                    """
                    UPDATE sensorthings."User"
                    SET uri = '/Users(' || id || ')'
                    WHERE id = $1
                    """,
                    new_user_id,
                )

                # 3c. Record the registration in the AuditLog — same
                #     transaction, so a logging failure rolls back the user
                #     row too (no silent orphans).
                await log_audit_event(
                    conn=conn,
                    action_type=AUDIT_ACTION_RESTRICTED_REQUEST,
                    actor_id=new_user_id,
                    dataset_id=request.dataset_id,
                    odrl_policy_id=request.odrl_policy_id,
                    payload={"explanation": request.explanation},
                )

        logger.info(
            "Restricted registration: new pending user '%s' (id=%d) "
            "requested access to dataset '%s' under policy '%s'.",
            request.username,
            new_user_id,
            request.dataset_id,
            request.odrl_policy_id,
        )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "id": new_user_id,
                "status": "pending",
                "message": (
                    f"Registration submitted. Your account (id={new_user_id}) "
                    "is pending administrator approval."
                ),
            },
        )

    except UniqueViolationError:
        logger.warning(
            "Registration rejected: username '%s' already exists.", request.username
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Username '{request.username}' is already taken.",
        )
    except (PostgresConnectionError, TooManyConnectionsError):
        logger.exception("Database unavailable during restricted registration")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"message": "Database temporarily unavailable."},
        )
    except QueryCanceledError:
        logger.exception("Database timeout during restricted registration")
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"message": "Database request timed out."},
        )
    except Exception:
        logger.exception("Unexpected error during restricted registration")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Internal server error."},
        )
