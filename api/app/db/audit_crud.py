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

"""Database helper for writing to the sensorthings."AuditLog" table.

Design decisions
----------------
* ``log_audit_event`` accepts a pre-acquired asyncpg connection (``conn``)
  rather than acquiring one from the pool itself.  This keeps the function
  composable: callers can include the audit INSERT in the same transaction
  as the action being logged, guaranteeing atomicity.

* asyncpg does not auto-serialize Python ``dict`` to PostgreSQL JSONB.
  Passing a raw dict raises ``TypeError: cannot convert dict to PostgreSQL
  type``.  The payload is serialised with ``json.dumps`` and the SQL
  parameter is cast explicitly as ``$6::jsonb``, following the pattern
  established in oidc_user_crud.py lines 104-106.

* ``None`` payload is passed through as-is; asyncpg maps Python ``None``
  to SQL ``NULL`` correctly without any special handling.

* The function is intentionally fire-and-log: it raises on hard DB errors
  so callers can decide whether to swallow or propagate.  Callers that
  treat audit logging as best-effort should wrap the call in try/except.
"""

import json
import logging

logger = logging.getLogger(__name__)

# Valid action_type values — must match the CHECK constraint in
# database/migrations/003_audit_log.sql.
AUDIT_ACTION_PUBLIC_READ = "PUBLIC_READ"
AUDIT_ACTION_RESTRICTED_REQUEST = "RESTRICTED_REQUEST"
AUDIT_ACTION_ADMIN_APPROVAL = "ADMIN_APPROVAL"


async def log_audit_event(
    conn,
    action_type: str,
    actor_id: int | None = None,
    dataset_id: str | None = None,
    odrl_policy_id: str | None = None,
    payload: dict | None = None,
) -> None:
    """Insert a single row into sensorthings."AuditLog".

    The table is append-only (UPDATE and DELETE are revoked at the DB level),
    so this function only ever issues an INSERT.

    Args:
        conn:            A live asyncpg connection (or transaction connection).
                         The caller is responsible for acquiring and releasing it.
        action_type:     One of AUDIT_ACTION_* constants above.  Must satisfy
                         the CHECK constraint in the migration or the INSERT
                         will raise a CheckViolationError.
        actor_id:        Primary key of the sensorthings."User" row that
                         triggered the event.  Pass ``None`` for anonymous
                         (unauthenticated) actions such as PUBLIC_READ.
        dataset_id:      Human-readable or URI identifier for the STAC dataset
                         being accessed or requested.  Optional.
        odrl_policy_id:  Identifier of the ODRL policy document associated
                         with a RESTRICTED_REQUEST or ADMIN_APPROVAL event.
                         Optional.
        payload:         Arbitrary JSON-serialisable metadata dict.  Optional.
                         asyncpg does not auto-serialise dicts to JSONB; this
                         function handles that conversion internally.

    Raises:
        asyncpg.CheckViolationError: if ``action_type`` is not one of the
            three permitted values.
        asyncpg.ForeignKeyViolationError: if ``actor_id`` does not reference
            an existing User row.
        Exception: any other asyncpg / database error.
    """
    # Serialise the payload dict to a JSON string so asyncpg can bind it as
    # JSONB.  Passing None through unchanged maps to SQL NULL.
    payload_json: str | None = json.dumps(payload) if payload is not None else None

    await conn.execute(
        """
        INSERT INTO sensorthings."AuditLog"
            (actor_id, action_type, dataset_id, odrl_policy_id, payload)
        VALUES
            ($1, $2, $3, $4, $5::jsonb)
        """,
        actor_id,
        action_type,
        dataset_id,
        odrl_policy_id,
        payload_json,
    )

    logger.info(
        "AuditLog: action=%r actor_id=%r dataset=%r policy=%r",
        action_type,
        actor_id,
        dataset_id,
        odrl_policy_id,
    )
