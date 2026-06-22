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

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Cached after first call. Import this everywhere instead of
    constructing Settings() directly.
    """
    return Settings()