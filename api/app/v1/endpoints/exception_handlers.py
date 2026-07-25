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

"""Central exception handlers for the STA write endpoints.

Registers one handler per driver/domain/validation exception on the v1 sub-app,
mapping each to the canonical STA error body via ``error_response``. This
replaces the near-identical ``try/except`` chains that were duplicated across
every create/update/delete endpoint: the endpoints now let these exceptions
propagate.

Handlers are registered on the ``v1`` FastAPI **sub-app** (the one mounted in
``main.py``), not the outer app -- a mounted sub-application handles its own
exceptions, so outer-app handlers would never fire for these routes.

Why letting exceptions propagate is safe:
- ``async with connection.transaction()`` rolls back when the exception
  propagates out of the endpoint, so no manual rollback is needed.
- The asyncpg pools are created without a custom ``reset``, so asyncpg runs
  ``RESET ALL`` when a connection is released to the pool. That clears any
  ``SET ROLE`` set during the request; the per-endpoint happy-path
  ``RESET ROLE`` is redundant and skipping it on the error path leaks no role.

asyncpg hierarchy note: ``ForeignKeyViolationError`` and ``UniqueViolationError``
subclass ``IntegrityConstraintViolationError``. Starlette resolves handlers by
walking the exception MRO most-specific-first, so the FK (400) and unique (409)
handlers win over the generic integrity handler; other integrity violations
(NOT NULL, CHECK, ...) fall through to it. ``STAError`` likewise wins over the
catch-all ``Exception`` handler.
"""

import logging

from asyncpg.exceptions import (
    DataError,
    ForeignKeyViolationError,
    InsufficientPrivilegeError,
    IntegrityConstraintViolationError,
    PostgresConnectionError,
    TooManyConnectionsError,
    UniqueViolationError,
)
from fastapi import FastAPI, Request, status

from app.v1.endpoints.error_response import error_response
from app.v1.endpoints.exceptions import STAError

logger = logging.getLogger(__name__)


async def handle_sta_error(request: Request, exc: STAError):
    # Domain error raised deliberately by endpoint/helper code; it carries the
    # HTTP status it maps to and a controlled, client-safe message.
    return error_response(exc.status_code, str(exc))


async def handle_insufficient_privilege(request: Request, exc: Exception):
    return error_response(
        status.HTTP_403_FORBIDDEN, "Insufficient privileges."
    )


async def handle_unique_violation(request: Request, exc: Exception):
    return error_response(status.HTTP_409_CONFLICT, "Entity already exists.")


async def handle_db_unavailable(request: Request, exc: Exception):
    # conformance: req/request-data/status-code -- DB unavailable is 503, not 400.
    return error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE, "Database temporarily unavailable"
    )


async def handle_foreign_key(request: Request, exc: Exception):
    # conformance: a bad @iot.id reference is a client error (400); controlled
    # message, never raw Postgres text.
    return error_response(
        status.HTTP_400_BAD_REQUEST, "Referenced entity does not exist."
    )


async def handle_integrity_violation(request: Request, exc: Exception):
    # conformance: a payload that violates a NOT NULL / CHECK / data constraint
    # is a client error (400), not a 500. Unique (409) and FK (400) are handled
    # by their own more-specific handlers above.
    return error_response(
        status.HTTP_400_BAD_REQUEST,
        "Invalid entity: a required value is missing or not allowed.",
    )


async def handle_value_error(request: Request, exc: Exception):
    # A stray ValueError is treated as a controlled client-error message. New
    # code should prefer raising BadRequest; this stays as a safety net.
    return error_response(status.HTTP_400_BAD_REQUEST, str(exc))


async def handle_internal_error(request: Request, exc: Exception):
    # conformance: req/request-data/status-code -- internal errors are 500 with
    # no stacktrace in the body. Log the real exception for operators.
    logger.exception(
        "Unhandled error in %s %s", request.method, request.url.path
    )
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error"
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Register the canonical STA write-error handlers on ``app`` (the v1 sub-app)."""
    app.add_exception_handler(STAError, handle_sta_error)
    app.add_exception_handler(
        InsufficientPrivilegeError, handle_insufficient_privilege
    )
    app.add_exception_handler(UniqueViolationError, handle_unique_violation)
    app.add_exception_handler(PostgresConnectionError, handle_db_unavailable)
    app.add_exception_handler(TooManyConnectionsError, handle_db_unavailable)
    app.add_exception_handler(ForeignKeyViolationError, handle_foreign_key)
    app.add_exception_handler(
        IntegrityConstraintViolationError, handle_integrity_violation
    )
    app.add_exception_handler(DataError, handle_integrity_violation)
    app.add_exception_handler(ValueError, handle_value_error)
    app.add_exception_handler(Exception, handle_internal_error)
