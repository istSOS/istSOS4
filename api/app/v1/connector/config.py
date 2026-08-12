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

Most settings are loaded through the Settings class (env vars / .env file),
so import get_settings() for those. A handful of flags below are read
straight off os.environ at import time instead, matching the rest of
istSOS's app/__init__.py -- they gate things (route enablement, auth) that
need to be checked before a request even reaches Settings-aware code.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Optional
    
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging

logger = logging.getLogger(__name__)


# Master switches for the two transformers, read once at import time.
# These gate whether the scheduler and API even attempt to touch STAC/DCAT
# at all, so they're plain module attributes rather than Settings fields --
# callers import them directly by name (from app.v1.connector.config
# import STAC_TRANSFORMER), same as the rest of this codebase.
STAC_TRANSFORMER: bool = os.getenv("STAC_TRANSFORMER", "0").strip() == "1"
DCAT_TRANSFORMER: bool = os.getenv("DCAT_TRANSFORMER", "0").strip() == "1"

# Defaults open (1). When AUTHORIZATION=1 and ANONYMOUS_VIEWER=0, setting
# this to 0 requires authentication even for the shallow tier (STAC root,
# /collections, /dcat/root etc.).
OPEN_CATALOG_METADATA: bool = os.getenv("OPEN_CATALOG_METADATA", "1").strip() == "1"

# Comma-separated integer network ids whose existence must not be leaked
# to unauthenticated callers (returns 404, not 401). Only meaningful when
# AUTHORIZATION=1 and ANONYMOUS_VIEWER=0. Malformed tokens are logged and
# skipped rather than raising, same warn-and-continue style as
# resolve_license_uri below.
def _parse_closed_networks() -> frozenset[int]:
    raw = os.getenv("CATALOG_CLOSED_NETWORKS", "").strip()
    if not raw:
        return frozenset()
    ids: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ids.add(int(token))
        except ValueError:
            logger.warning(
                "CATALOG_CLOSED_NETWORKS: ignoring malformed token %r (expected an integer network id)",
                token,
            )
    return frozenset(ids)


CATALOG_CLOSED_NETWORKS: frozenset[int] = _parse_closed_networks()

_STAC_NON_SPDX_LICENSES = {"various", "proprietary"}
_STAC_LICENSE_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-+]*$")

SPDX_LICENSE_URIS: dict[str, str] = {
    "CC-BY-4.0": "https://spdx.org/licenses/CC-BY-4.0",
    "CC-BY-SA-4.0": "https://spdx.org/licenses/CC-BY-SA-4.0",
    "CC-BY-NC-4.0": "https://spdx.org/licenses/CC-BY-NC-4.0",
    "CC-BY-NC-SA-4.0": "https://spdx.org/licenses/CC-BY-NC-SA-4.0",
    "CC0-1.0": "https://spdx.org/licenses/CC0-1.0",
    "ODbL-1.0": "https://spdx.org/licenses/ODbL-1.0",
    "ODC-By-1.0": "https://spdx.org/licenses/ODC-By-1.0",
    "MIT": "https://spdx.org/licenses/MIT",
    "Apache-2.0": "https://spdx.org/licenses/Apache-2.0",
}

_LICENSE_PLACEHOLDERS = {"other", "unknown", "none", "n/a", "na", "tbd", ""}


def resolve_license_uri(value: str | None, *, context: str = "") -> str | None:
    """
    Normalize a license value (from DCAT_DEFAULT_LICENSE or a Datastream's
    own properties["license"]) into a URI safe to wrap in rdflib.URIRef.

    None/empty/placeholder -> None (logged). Absolute URI -> unchanged.
    Known SPDX id -> mapped to its spdx.org URI. Anything else -> best
    guess at "https://spdx.org/licenses/{token}", logged as a warning.
    `context` (e.g. "Datastream 771") is just for the log message.
    """
    if not value:
        return None

    v = value.strip()
    suffix = f" ({context})" if context else ""

    if v.lower() in _LICENSE_PLACEHOLDERS:
        logger.warning(
            "Ignoring placeholder license value %r%s -- no dct:license will be emitted",
            value, suffix,
        )
        return None

    if v.startswith("http://") or v.startswith("https://"):
        return v

    if v in SPDX_LICENSE_URIS:
        return SPDX_LICENSE_URIS[v]

    guessed = f"https://spdx.org/licenses/{v}"
    logger.warning(
        "License value %r%s is not an absolute URI and not in SPDX_LICENSE_URIS -- "
        "guessing %s. Add it to SPDX_LICENSE_URIS to silence this warning.",
        value, suffix, guessed,
    )
    return guessed


# EU Publications Office Named Authority List (NAL) for languages, keyed
# by the BCP-47 tag DCAT_LANGUAGE is set to. DCAT-AP 3.0 requires
# dct:language to be a skos:Concept URI from this list, not a bare
# literal. Extend as new deployments need more languages.
EU_LANGUAGE_AUTHORITY_URIS: dict[str, str] = {
    "en": "http://publications.europa.eu/resource/authority/language/ENG",
    "it": "http://publications.europa.eu/resource/authority/language/ITA",
    "de": "http://publications.europa.eu/resource/authority/language/DEU",
    "fr": "http://publications.europa.eu/resource/authority/language/FRA",
    "es": "http://publications.europa.eu/resource/authority/language/SPA",
}


def resolve_language_uri(value: str | None) -> str | None:
    """
    Normalize a BCP-47 language tag (typically settings.DCAT_LANGUAGE) into
    the EU NAL language URI that dct:language must point at.

    None/empty -> None. Absolute URI -> unchanged. Known tag (case
    insensitive) -> mapped to its authority URI. Anything else -> None,
    logged as a warning -- unlike SPDX ids, NAL language codes don't
    follow a predictable pattern, so we don't guess.
    """
    if not value:
        return None

    v = value.strip()

    if v.startswith("http://") or v.startswith("https://"):
        return v

    uri = EU_LANGUAGE_AUTHORITY_URIS.get(v.lower())
    if uri is None:
        logger.warning(
            "DCAT_LANGUAGE=%r has no entry in EU_LANGUAGE_AUTHORITY_URIS -- "
            "no dct:language triple will be emitted. Add it to the table "
            "(or set DCAT_LANGUAGE to the full authority URI directly) to fix this.",
            value,
        )
        return None
    return uri


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
        description="id of the root STAC Catalog. Must be unique if more than one connector instance is aggregated by the same STAC client.",
    )
    STAC_CATALOG_TITLE: Optional[str] = Field(
        default=None,
        description="Optional human-readable title for the root STAC Catalog.",
    )
    STAC_DEPLOYMENT_NAME: str = Field(
        default="istSOS4",
        description="Deployment name interpolated into the root Catalog's description text.",
    )
    STAC_DEFAULT_LICENSE: str = Field(
        default="proprietary",
        description="Fallback Collection.license when a Thing carries no license. Must be an SPDX id, 'various', or 'proprietary' per STAC 1.0.",
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

    # DCAT-AP 3.0 catalog identity
    DCAT_CATALOG_ID: str = Field(
        default="istsos-connector-dcat-catalog",
        description="dct:identifier of the root dcat:Catalog. Independent from STAC_CATALOG_ID.",
    )
    DCAT_CATALOG_TITLE: Optional[str] = Field(
        default=None,
        description="dct:title of the root dcat:Catalog. Mandatory per DCAT-AP 3.0 -- see has_mandatory_dcat_fields.",
    )
    DCAT_CATALOG_DESCRIPTION: Optional[str] = Field(
        default=None,
        description="dct:description of the root dcat:Catalog. Mandatory per DCAT-AP 3.0.",
    )
    DCAT_DEPLOYMENT_NAME: str = Field(
        default="istSOS4",
        description="Deployment name interpolated into composed dct:description text.",
    )
    DCAT_LANGUAGE: str = Field(
        default="en",
        description="BCP-47 language tag used on every language-tagged rdflib.Literal (dct:title, dct:description, dcat:keyword, ...).",
    )

    # DCAT-AP licensing / rights
    # TODO: resolve_license_uri accepts a bare SPDX token; DCAT_DEFAULT_LICENSE
    # currently must already be a resolvable URI. Route it through the same
    # resolver as STAC_DEFAULT_LICENSE so operators can set one value for both.
    DCAT_DEFAULT_LICENSE: Optional[str] = Field(
        default=None,
        description="Fallback dct:license URI for Datasets/Distributions with no license of their own. Unset by default -- no triple emitted until set.",
    )
    DCAT_DEFAULT_ACCESS_RIGHTS: Optional[str] = Field(
        default=None,
        description="Fallback dct:accessRights URI (e.g. an MDR-AccessRights value like PUBLIC) for Datastreams with no accessRights of their own.",
    )

    # DCAT-AP publisher agent
    DCAT_PUBLISHER_NAME: Optional[str] = Field(
        default=None,
        description="foaf:name of the publisher Agent. Mandatory per DCAT-AP 3.0 -- see has_mandatory_dcat_fields.",
    )
    DCAT_PUBLISHER_URI: Optional[str] = Field(
        default=None,
        description="URI identifying the publisher Agent as a named node. When unset but DCAT_PUBLISHER_NAME is set, emitted as a BNode instead (logged).",
    )
    DCAT_PUBLISHER_HOMEPAGE: Optional[str] = Field(default=None)
    DCAT_PUBLISHER_MBOX: Optional[str] = Field(
        default=None,
        description="Publisher contact email. 'mailto:' is prepended automatically if not already present.",
    )

    @property
    def has_mandatory_dcat_fields(self) -> bool:
        """True when every DCAT-AP 3.0 mandatory Catalog field with no STA source (title, description, publisher) is set."""
        return bool(
            self.DCAT_CATALOG_TITLE
            and self.DCAT_CATALOG_DESCRIPTION
            and self.DCAT_PUBLISHER_NAME
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Cached after first call. Import this everywhere instead of
    constructing Settings() directly.
    """
    return Settings()