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

"""CRUD helpers for OIDC-linked user provisioning.

These functions are the **only** permitted path for inserting or updating a
user record that originates from an external identity provider.  They are
intentionally narrow:

* ``create_pending_oidc_user`` – inserts a new row with role='pending'.
  It deliberately contains NO ``CREATE ROLE`` / ``CREATE USER`` DDL; the new
  account has zero PostgreSQL database footprint until an administrator calls
  the ``/Users/{id}/activate`` endpoint.

* ``get_user_by_provider_sub`` – idempotency look-up used by the OIDC callback
  route to check whether a provider+sub pair already exists before inserting.
"""

import logging

from app.db.asyncpg_db import get_pool
from app.rbac_roles import PENDING_ROLE
from asyncpg.exceptions import UniqueViolationError

logger = logging.getLogger(__name__)


async def get_user_by_provider_sub(
    auth_provider: str,
    external_sub_id: str,
) -> dict | None:
    """Look up an existing user by their external-provider subject identifier.

    Returns a dict with ``{id, username, role, uri, auth_provider,
    external_sub_id}`` or ``None`` if no match is found.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, role, uri, auth_provider, external_sub_id
            FROM sensorthings."User"
            WHERE auth_provider = $1
              AND external_sub_id = $2
            """,
            auth_provider,
            external_sub_id,
        )
    if row is None:
        return None
    return dict(row)


async def create_pending_oidc_user(
    username: str,
    email: str | None,
    auth_provider: str,
    external_sub_id: str,
) -> dict:
    """Insert a new OIDC-linked user in the 'pending' waiting room.

    The row is inserted with ``role = PENDING_ROLE`` ('pending') so that
    ``get_current_user`` will immediately gate any request from this account
    with HTTP 403 until an administrator activates it.

    Security guarantees:
      - Role is **hardcoded** to ``PENDING_ROLE`` — callers cannot override it.
      - NO ``CREATE ROLE`` / ``CREATE USER`` PostgreSQL commands are executed.
        The new account has zero database footprint until activation.
      - ``auth_provider`` and ``external_sub_id`` are passed as asyncpg
        ``$N`` parameters — no string interpolation, no SQL-injection risk.

    Args:
        username:        Preferred display name / login handle (from OIDC
                         ``preferred_username`` or ``name`` claim).
        email:           Email from the OIDC ``email`` claim; stored as part
                         of the ``contact`` JSON blob. May be ``None``.
        auth_provider:   Short identifier for the IdP, e.g. ``"google"``,
                         ``"orcid"``, ``"keycloak"``.
        external_sub_id: The ``sub`` claim from the provider's JWT — globally
                         unique within that provider's namespace.

    Returns:
        dict with keys ``id``, ``username``, ``role``, ``uri``,
        ``auth_provider``, ``external_sub_id``.

    Raises:
        UniqueViolationError: if the (auth_provider, external_sub_id) pair or
            the username already exists (caller should treat this as a no-op /
            return the existing record via ``get_user_by_provider_sub``).
        Exception: any other asyncpg / database error bubbles up.
    """
    import json as _json

    contact = _json.dumps({"email": email}) if email else None

    pool = await get_pool()
    async with pool.acquire() as conn:
        # INSERT only — no DDL, no GRANT, no CREATE ROLE.
        row = await conn.fetchrow(
            """
            INSERT INTO sensorthings."User"
                (username, contact, role, auth_provider, external_sub_id)
            VALUES
                ($1, $2::jsonb, $3, $4, $5)
            RETURNING id, username, role, uri, auth_provider, external_sub_id;
            """,
            username,
            contact,
            PENDING_ROLE,   # hardcoded — never accept from caller
            auth_provider,
            external_sub_id,
        )

    logger.info(
        "JIT-provisioned pending OIDC user: username=%r provider=%r",
        username,
        auth_provider,
    )
    return dict(row)
