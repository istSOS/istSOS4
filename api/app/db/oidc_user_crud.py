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

import hashlib
import logging

from app.db.asyncpg_db import get_pool
from app.rbac_roles import PENDING_ROLE
from asyncpg.exceptions import UniqueViolationError

logger = logging.getLogger(__name__)

# Constraint name from the base sensorthings."User" table definition —
# used to distinguish a username collision from a (provider, sub)
# collision, since a UniqueViolationError alone doesn't say which.
# The other unique constraint on this table is
# "uq_user_auth_provider_sub_id" (auth_provider, external_sub_id) —
# any UniqueViolationError NOT matching the constant below is assumed
# to be that one and re-raised unchanged.
_USERNAME_UNIQUE_CONSTRAINT = "User_username_key"

# Number of candidate usernames tried before giving up: the base name,
# then up to three numeric suffixes, then one hash-based suffix. See
# _suffixed_candidate().
_MAX_USERNAME_ATTEMPTS = 5


def _suffixed_candidate(base: str, seed: str, attempt: int) -> str:
    """Return the *attempt*-th fallback username when *base* is taken.

    attempt 1-3 append a short numeric suffix (_2, _3, _4) — cheap and
    matches how GitHub/Slack/Discourse resolve a colliding handle: this
    is a cosmetic uniqueness clash, not an identity question, so there is
    nothing to ask the applicant and no reason to block signup over it.

    attempt 4 (the last one tried) appends a short deterministic hash of
    *seed* (pass the provider's `sub` claim) instead of a numeric suffix,
    so a second callback for the same identity — e.g. a user reloading a
    stuck browser tab — lands on the same candidate rather than a new
    random one each time.

    Truncates *base* so the result still fits validate_username()'s
    63-character limit.
    """
    if attempt < _MAX_USERNAME_ATTEMPTS - 1:
        suffix = f"_{attempt + 1}"
    else:
        suffix = "_" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8]
    return base[: 63 - len(suffix)] + suffix


class OidcUsernameCollisionError(Exception):
    """Raised only if *every* candidate username — the base name plus all
    of _suffixed_candidate()'s fallbacks — collides with an existing
    account. In practice this means the same (astronomically unlikely)
    hash-suffixed candidate also collided, not just the base name; a
    plain collision on the base name is now resolved automatically by
    create_pending_oidc_user() instead of raising this.
    """


async def get_user_by_provider_sub(
    auth_provider: str,
    external_sub_id: str,
) -> dict | None:
    """Look up an existing user by their external-provider subject identifier.

    Returns a dict with ``{id, username, role, uri, auth_provider,
    external_sub_id, dataset_id, odrl_policy_id}`` or ``None`` if no match
    is found.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, role, uri, auth_provider, external_sub_id,
                dataset_id, odrl_policy_id
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
    dataset_id: str | None = None,
    odrl_policy_id: str | None = None,
    requested_role: str | None = None,
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
        dataset_id:      Dataset the user selected before starting the OIDC
                         handshake (see app.v1.endpoints.create.oidc_login),
                         mirroring what POST /Register already collects for
                         local accounts. May be ``None`` if the caller never
                         collected one.
        odrl_policy_id:  ODRL policy identifier tied to that dataset
                         selection. Same nullability as dataset_id.
        requested_role:  RBAC role the user asked to be granted, collected
                         the same way as dataset_id/odrl_policy_id (see
                         oidc_login.py). An administrator reviewing the
                         pending queue sees this as the default at
                         activation time but can assign a different role —
                         see activate_user.py. Same nullability as
                         dataset_id.

    Returns:
        dict with keys ``id``, ``username``, ``role``, ``uri``,
        ``auth_provider``, ``external_sub_id``, ``dataset_id``,
        ``odrl_policy_id``. ``username`` reflects whichever candidate
        actually got inserted — the caller's ``username`` argument if it
        was free, or an auto-suffixed variant otherwise (see
        _suffixed_candidate()).

    Raises:
        OidcUsernameCollisionError: only if ``username`` and every
            auto-suffixed fallback (see _suffixed_candidate()) all
            collide with existing accounts — vanishingly rare in
            practice, since the last fallback is a hash of the caller's
            own ``external_sub_id``.
        UniqueViolationError: if the (auth_provider, external_sub_id) pair
            already exists (caller should treat this as a no-op / return
            the existing record via ``get_user_by_provider_sub``).
        Exception: any other asyncpg / database error bubbles up.
    """
    import json as _json

    contact = _json.dumps({"email": email}) if email else None

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Advisory-only duplicate hint: does any existing, unrelated
        # account already use this same email? Not account linking — no
        # login behaviour changes, nothing is merged. It's a plain FK an
        # administrator sees on the pending queue (GET /Users) alongside
        # the "id" column already returned there, letting a human make
        # the "is this the same person" call rather than the system
        # guessing. See 009_oidc_duplicate_hint.sql.
        possible_duplicate_of = None
        if email:
            dup_row = await conn.fetchrow(
                """
                SELECT id FROM sensorthings."User"
                WHERE contact ->> 'email' = $1
                LIMIT 1;
                """,
                email,
            )
            if dup_row is not None:
                possible_duplicate_of = dup_row["id"]

        # Auto-suffix on a plain username collision instead of rejecting
        # the signup: two unrelated people whose handles happen to match
        # is a cosmetic uniqueness clash, not an identity question (see
        # _suffixed_candidate()). Whether this candidate is ALSO a likely
        # duplicate identity is a separate concern, already captured
        # above via possible_duplicate_of for an administrator to judge.
        last_exc = None
        for attempt in range(_MAX_USERNAME_ATTEMPTS):
            candidate = (
                username
                if attempt == 0
                else _suffixed_candidate(username, external_sub_id, attempt)
            )

            # INSERT + audit write share one transaction, same reasoning
            # as create/register_request.py: a logging failure must not
            # leave an unaudited pending account behind.
            try:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        INSERT INTO sensorthings."User"
                            (username, contact, role, auth_provider,
                             external_sub_id, dataset_id, odrl_policy_id,
                             requested_role, possible_duplicate_of)
                        VALUES
                            ($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9)
                        RETURNING id, username, role, uri, auth_provider,
                            external_sub_id, dataset_id, odrl_policy_id;
                        """,
                        candidate,
                        contact,
                        PENDING_ROLE,  # hardcoded — never accept from caller
                        auth_provider,
                        external_sub_id,
                        dataset_id,
                        odrl_policy_id,
                        requested_role,
                        possible_duplicate_of,
                    )

                    # Lazy import: audit_crud has no reverse dependency on
                    # this module, so this just avoids a module-load-order
                    # footgun rather than a real circular import.
                    from app.db.audit_crud import (
                        AUDIT_ACTION_RESTRICTED_REQUEST,
                        log_audit_event,
                    )

                    await log_audit_event(
                        conn=conn,
                        action_type=AUDIT_ACTION_RESTRICTED_REQUEST,
                        actor_id=row["id"],
                        dataset_id=dataset_id,
                        odrl_policy_id=odrl_policy_id,
                        payload={
                            "auth_provider": auth_provider,
                            "requested_role": requested_role,
                        },
                    )
                break
            except UniqueViolationError as exc:
                if exc.constraint_name != _USERNAME_UNIQUE_CONSTRAINT:
                    # (provider, sub) collision or anything else —
                    # unchanged contract, caller recovers via
                    # get_user_by_provider_sub.
                    raise
                last_exc = exc
                if candidate != username:
                    logger.info(
                        "OIDC username candidate %r also collided, "
                        "trying next fallback (attempt %d/%d)",
                        candidate,
                        attempt + 1,
                        _MAX_USERNAME_ATTEMPTS,
                    )
        else:
            raise OidcUsernameCollisionError(
                f"OIDC provisioning failed: username {username!r} caused a "
                f"collision, and all {_MAX_USERNAME_ATTEMPTS - 1} "
                "auto-suffixed fallbacks also collided with existing "
                "accounts."
            ) from last_exc

    logger.info(
        "JIT-provisioned pending OIDC user: username=%r provider=%r",
        row["username"],
        auth_provider,
    )
    return dict(row)
