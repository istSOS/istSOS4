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

"""Domain exceptions for the STA endpoints.

Each exception carries the HTTP status it maps to, so endpoint and helper code
can ``raise BadRequest("...")`` and let it propagate: a single handler
(``handle_sta_error`` in exception_handlers.py) turns any ``STAError`` into the
canonical STA error body with the right status. This replaces the previous
``raise Exception("...")`` pattern, which the catch-all handler surfaced as a
500 even though the condition (a missing id, a bad body, a role restriction) is
a client error.
"""


class STAError(Exception):
    """Base for controlled STA errors that map to a specific HTTP status.

    Subclasses set ``status_code`` (and optionally ``default_message``). Raise
    with an explicit message, or bare to use the subclass default.
    """

    status_code = 500
    default_message = "Internal server error"

    def __init__(self, message: str | None = None):
        self.message = message if message is not None else self.default_message
        super().__init__(self.message)


class BadRequest(STAError):
    status_code = 400
    default_message = "Bad request"


class Unauthorized(STAError):
    status_code = 401
    default_message = "Unauthorized"


class Forbidden(STAError):
    status_code = 403
    default_message = "Insufficient privileges."


class NotFound(STAError):
    status_code = 404
    default_message = "Not found"


class Conflict(STAError):
    status_code = 409
    default_message = "Entity already exists."


class ServiceUnavailable(STAError):
    status_code = 503
    default_message = "Database temporarily unavailable"
