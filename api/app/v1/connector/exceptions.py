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
Harvester exception hierarchy.

All exceptions raised by the harvesting layer are subclasses of
HarvesterError so callers can catch the base type or individual subtypes
depending on how fine-grained their error handling needs to be.
"""

from __future__ import annotations


class HarvesterError(Exception):
    """Base class for all harvester exceptions."""


class HarvesterQueryError(HarvesterError):
    """
    Raised when the asyncpg JOIN query itself fails (connection lost,
    bad SQL, permissions, pool exhausted, etc). Not retried inside
    harvest() -- a failed cycle is caught by scheduled_harvest_job(),
    which simply skips the cycle and leaves the previous valid cache
    in Redis untouched.
    """


class HarvesterRowError(HarvesterError):
    """
    Raised when a returned row cannot be normalised into the expected
    shape (for example a row missing both thing_id and any other
    identifying column). Distinct from HarvesterQueryError because the
    query itself succeeded; only row-level normalisation failed.
    """