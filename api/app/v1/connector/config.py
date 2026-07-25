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
Connector configuration layer.

All settings are loaded from environment variables or a .env file in the
project root. The Settings class is the single source of truth for every
configurable value. Import get_settings() everywhere, never read
os.environ directly.

It reads Postgres directly through a pool that istSOS already owns and constructs. 
Redis keys also carry no TTL of their own anymore (see Harvesting-Layer-Reference.md),
so CACHE_TTL_SECONDS is gone too. The only new setting is HARVEST_INTERVAL_MINUTES, 
read by the APScheduler registration in istSOS's main.py.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_STAC_NON_SPDX_LICENSES = {"various", "proprietary"}
_STAC_LICENSE_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-+]*$")


class Settings(BaseSettings):
    """Connector configuration loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        extra="ignore",
    )

    # Scheduling
    HARVEST_INTERVAL_MINUTES: int = Field(
        default=5,
        description=(
            "How often scheduled_harvest_job() fires. Read by the "
            "APScheduler registration in istSOS main.py, not by the "
            "connector package itself."
        ),
    )

    # STAC catalog identity
    STAC_CATALOG_ID: str = Field(
        default="istsos-connector-catalog",
        description=(
            "id of the root STAC Catalog. Must be unique if more than one "
            "instance of this connector is ever aggregated by the same "
            "STAC client (e.g. a multi-deployment eoAPI browser)."
        ),
    )
    STAC_CATALOG_TITLE: Optional[str] = Field(
        default=None,
        description=(
            "Optional human-readable title for the root STAC Catalog. "
            "Falls back to no title (STAC Catalog.title is optional) when unset."
        ),
    )
    STAC_DEPLOYMENT_NAME: str = Field(
        default="istSOS4",
        description=(
            "Deployment name interpolated into the root Catalog's "
            "description text, e.g. '<name> deployment: N Things...'. "
            "Set this to something identifying (site name, org name) once "
            "more than one deployment exists."
        ),
    )
    STAC_DEFAULT_LICENSE: str = Field(
        default="proprietary",
        description=(
            "Fallback Collection.license used when a Thing's own "
            "properties carry no license. Per STAC 1.0 this must be an "
            "SPDX identifier (e.g. 'CC-BY-4.0'), 'various', or "
            "'proprietary' -- nothing else validates against the spec."
        ),
    )

    @field_validator("STAC_DEFAULT_LICENSE")
    @classmethod
    def _validate_stac_default_license(cls, v: str) -> str:
        if v in _STAC_NON_SPDX_LICENSES:
            return v
        if not _STAC_LICENSE_TOKEN_PATTERN.match(v):
            raise ValueError(
                f"STAC_DEFAULT_LICENSE={v!r} is not a valid STAC 1.0 license "
                "value. Use an SPDX identifier (e.g. 'CC-BY-4.0', 'MIT'), "
                "'various', or 'proprietary'. Free-text values like 'other' "
                "are not spec-conformant and will misbehave in STAC clients."
            )
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Cached after first call. Import this everywhere instead of
    constructing Settings() directly.
    """
    return Settings()