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

import os
import re
from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging
 
logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    """
    Parse a 0/1-style boolean env var. Deliberately NOT `bool(os.getenv(name))`
    -- that treats any non-empty string as truthy, so an explicit "0" in
    .env would evaluate to True, which is exactly backwards for a master
    switch that must default off and require an explicit opt-in.
    """
    return os.getenv(name, default).strip() == "1"


# Master switches for the two transformers. Read once at import time via
# plain os.getenv rather than as Settings fields -- these gate whether the
# scheduler and API even attempt to touch STAC/DCAT at all, so unlike the
# rest of this file there's no need to inject them as instance state
# anywhere. Centralized here (rather than duplicated in scheduler.py and
# api.py, the two places that need them) purely so both read the same
# parsed value instead of risking two slightly different os.getenv calls
# drifting apart over time.
STAC_TRANSFORMER: bool = _env_flag("STAC_TRANSFORMER")
DCAT_TRANSFORMER: bool = _env_flag("DCAT_TRANSFORMER")

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

    Resolution order:
    1. None / empty / known placeholder ("other", "unknown", ...)
        -> None. Warning logged. No dct:license triple should be emitted.
    2. Already an absolute URI ("http://..." / "https://...")
        -> returned unchanged.
    3. A bare SPDX id present in SPDX_LICENSE_URIS
        -> mapped to its canonical spdx.org URI.
    4. Any other bare token
        -> best-effort guess at "https://spdx.org/licenses/{token}",
        with a warning, so an unmapped-but-valid SPDX id (or a typo)
        is visible in logs rather than silently wrong.

    `context` is free text (e.g. "Datastream 771") purely for the log
    message, so a bad value in production points straight at its source.

    Module-level on purpose (not a Settings method) -- dcat_transformer.py
    imports it directly as `from app.v1.connector.config import
    resolve_license_uri`, and it needs no `self`/instance state to run.
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

    # DCAT-AP 3.0 catalog identity
    DCAT_CATALOG_ID: str = Field(
        default="istsos-connector-dcat-catalog",
        description=(
            "dct:identifier of the root dcat:Catalog. Independent from "
            "STAC_CATALOG_ID -- the two standards are served as separate "
            "catalogs and are allowed to diverge."
        ),
    )
    DCAT_CATALOG_TITLE: Optional[str] = Field(
        default=None,
        description=(
            "dct:title of the root dcat:Catalog. Mandatory per DCAT-AP 3.0. "
            "When unset, has_mandatory_dcat_fields is False and the "
            "transformer emits a partial graph with a logged warning."
        ),
    )
    DCAT_CATALOG_DESCRIPTION: Optional[str] = Field(
        default=None,
        description=(
            "dct:description of the root dcat:Catalog. Mandatory per "
            "DCAT-AP 3.0, same partial-graph behavior as DCAT_CATALOG_TITLE "
            "when unset."
        ),
    )
    DCAT_DEPLOYMENT_NAME: str = Field(
        default="istSOS4",
        description=(
            "Deployment name interpolated into composed dct:description "
            "text, mirrors STAC_DEPLOYMENT_NAME's role for the STAC side."
        ),
    )
    DCAT_LANGUAGE: str = Field(
        default="en",
        description=(
            "BCP-47 language tag used on every rdflib.Literal with a "
            "language-tagged string (dct:title, dct:description, "
            "dcat:keyword, ...). Single-language deployment for now; "
            "multi-language support is a future extension."
        ),
    )

    # DCAT-AP licensing / rights
    # TODO: DCAT_DEFAULT_LICENSE is a placeholder. dct:license needs to
    # resolve to a real license URI (e.g. https://spdx.org/licenses/CC-BY-4.0),
    # unlike STAC_DEFAULT_LICENSE which accepts a bare SPDX token. Build a
    # small SPDX-id -> URI mapping table so operators can keep setting one
    # license identifier and have both standards derive a spec-conformant
    # value from it, instead of maintaining two separate license settings.
    DCAT_DEFAULT_LICENSE: Optional[str] = Field(
        default=None,
        description=(
            "Fallback dct:license URI applied to every Dataset and "
            "Distribution when a Datastream's own properties carry no "
            "license. Must be a resolvable URI, not an SPDX token -- see "
            "the TODO above. Left unset (None) by default; no dct:license "
            "triple is emitted until an operator sets this."
        ),
    )
    DCAT_DEFAULT_ACCESS_RIGHTS: Optional[str] = Field(
        default=None,
        description=(
            "Fallback dct:accessRights URI (e.g. a MDR-AccessRights "
            "authority-table value such as PUBLIC) applied when a "
            "Datastream's own properties carry no accessRights value."
        ),
    )

    # DCAT-AP publisher agent
    DCAT_PUBLISHER_NAME: Optional[str] = Field(
        default=None,
        description=(
            "foaf:name of the publisher Agent. dct:publisher is mandatory "
            "per DCAT-AP 3.0 on the root Catalog; leaving this unset means "
            "has_mandatory_dcat_fields is False and no publisher node is "
            "emitted at all."
        ),
    )
    DCAT_PUBLISHER_URI: Optional[str] = Field(
        default=None,
        description=(
            "URI identifying the publisher Agent as a named node. When "
            "unset but DCAT_PUBLISHER_NAME is set, the publisher is "
            "emitted as a BNode -- valid RDF, but not referenceable from "
            "outside this graph, so a warning is logged."
        ),
    )
    DCAT_PUBLISHER_HOMEPAGE: Optional[str] = Field(default=None)
    DCAT_PUBLISHER_MBOX: Optional[str] = Field(
        default=None,
        description=(
            "Publisher contact email. 'mailto:' is prepended automatically "
            "if not already present."
        ),
    )

    @property
    def has_mandatory_dcat_fields(self) -> bool:
        """
        True only when every DCAT-AP 3.0 mandatory Catalog field that has
        no STA source (title, description, publisher) has been configured.
        Callers may gate on this before invoking the transformer; the
        transformer itself always produces a graph regardless, logging a
        warning when this is False.
        """
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
