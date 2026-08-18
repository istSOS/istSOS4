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
Scheduling layer for the istSOS Metadata Connector.

Immediate first run: start_scheduler() forces an immediate first fire
(next_run_time=now) and then settles into the normal interval after that.

Cache writes: stac and dcat are written within one cycle, or neither is,
per standard (each is independent of the other -- a DCAT failure does not
roll back the STAC write, and vice versa; see _run_cycle).

Public interface:
    start_scheduler(pool) -> AsyncIOScheduler
"""

from __future__ import annotations

import logging
from datetime import datetime

import asyncpg
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.v1.connector.cache import (
    write_stac_catalog,
    write_stac_catalog_with_networks,
    write_dcat_catalog,
    write_dcat_catalog_with_networks,
)
from app.v1.connector.config import get_settings, STAC_TRANSFORMER, DCAT_TRANSFORMER
from app.v1.connector.harvester import harvest, harvest_with_networks
from app.v1.connector.stac_transformer import build_stac_catalog, build_stac_catalog_with_networks
from app.v1.connector.dcat_transformer import build_dcat_catalog, build_dcat_catalog_with_networks

logger = logging.getLogger(__name__)

_HARVEST_LOCK_KEY = 726419950_1
NETWORK = int(os.getenv("NETWORK", "1"))

_dcat_misconfig_warned = False


def _missing_dcat_mandatory_fields(config) -> list[str]:
    """Names of the DCAT-AP 3.0 mandatory fields that are currently unset."""
    missing = []
    if not config.DCAT_CATALOG_TITLE:
        missing.append("DCAT_CATALOG_TITLE")
    if not config.DCAT_CATALOG_DESCRIPTION:
        missing.append("DCAT_CATALOG_DESCRIPTION")
    if not config.DCAT_PUBLISHER_NAME:
        missing.append("DCAT_PUBLISHER_NAME")
    return missing


def start_scheduler(pool: asyncpg.Pool) -> AsyncIOScheduler:
    """
    Build, start, and return an AsyncIOScheduler running the harvest cycle.

    Fires once immediately (so the cache is populated before the first
    request arrives, not up to HARVEST_INTERVAL_MINUTES later), then every
    HARVEST_INTERVAL_MINUTES after that.

    Called once from main.py's lifespan, with the pool main.py already
    constructs via initialize_pool()/get_pool(). The returned scheduler is
    main.py's to shut down on application teardown.
    """
    config = get_settings()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scheduled_harvest_job,
        trigger=IntervalTrigger(minutes=config.HARVEST_INTERVAL_MINUTES),
        args=[pool],
        next_run_time=datetime.now(),
        id="connector_harvest_cycle",
        max_instances=1,
    )
    scheduler.start()
    logger.info(
        "Connector scheduler started -- harvest cycle every %d minutes, first run immediate",
        config.HARVEST_INTERVAL_MINUTES,
    )
    logger.info(
        "STAC_TRANSFORMER=%s, DCAT_TRANSFORMER=%s",
        int(STAC_TRANSFORMER), int(DCAT_TRANSFORMER),
    )
    if DCAT_TRANSFORMER and not config.has_mandatory_dcat_fields:
        logger.warning(
            "DCAT_TRANSFORMER=1 but DCAT-AP 3.0 mandatory fields are unset: %s -- "
            "DCAT transform/write will be skipped every cycle until these are set",
            ", ".join(_missing_dcat_mandatory_fields(config)),
        )
    return scheduler


async def scheduled_harvest_job(pool: asyncpg.Pool) -> None:
    """
    Run one full harvest cycle: acquire advisory lock, harvest, transform,
    write cache, release lock.

    If the lock is not acquired (another worker is already mid-cycle),
    this cycle is skipped entirely.

    Any exception during harvest or transform is caught and logged; the
    cycle is abandoned and the previous valid Redis cache is left untouched.
    """
    config = get_settings()

    async with pool.acquire() as lock_conn:
        acquired = await lock_conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", _HARVEST_LOCK_KEY
        )
        if not acquired:
            logger.info("Harvest cycle skipped -- advisory lock held by another worker")
            return

        try:
            async with pool.acquire() as harvest_conn:
                await _run_cycle(harvest_conn, config)
        except Exception:
            logger.exception("Harvest cycle failed -- previous cache left untouched")
        finally:
            await lock_conn.fetchval(
                "SELECT pg_advisory_unlock($1)", _HARVEST_LOCK_KEY
            )


async def _run_cycle(connection: asyncpg.Connection, config) -> None:
    """
    Run harvest + both transforms + both cache writes, holding the
    advisory lock for the whole duration. Raises on any failure so the
    caller's except block can log and skip the cycle cleanly.

    STAC only builds/writes when STAC_TRANSFORMER=1. DCAT only builds/
    writes when DCAT_TRANSFORMER=1 AND config.has_mandatory_dcat_fields is
    True -- the transformer itself never refuses to run (see its own
    docstring), so this check has to happen here, before it's ever
    invoked, or a misconfigured deployment would cache and serve a
    DCAT-AP-nonconformant partial graph every cycle. The two standards
    remain independent of each other -- a DCAT failure will not roll back
    the STAC write, and vice versa.

    NETWORK dispatch mirrors STAC's exactly: harvest() and
    build_stac_catalog()/build_dcat_catalog() for NETWORK=0,
    harvest_with_networks() and the *_with_networks() variants for
    NETWORK=1. Both standards are built from the same single harvest call
    per cycle -- there is no second Postgres round trip for DCAT.
    """
    global _dcat_misconfig_warned

    if not STAC_TRANSFORMER and not DCAT_TRANSFORMER:
        logger.debug(
            "Harvest cycle skipped -- both STAC_TRANSFORMER and DCAT_TRANSFORMER are 0"
        )
        return

    dcat_ready = DCAT_TRANSFORMER and config.has_mandatory_dcat_fields

    if DCAT_TRANSFORMER and not config.has_mandatory_dcat_fields and not _dcat_misconfig_warned:
        logger.warning(
            "DCAT transform/write skipped -- DCAT_TRANSFORMER=1 but mandatory "
            "fields are unset: %s. This warning will not repeat every cycle; "
            "fix the config and restart to clear it.",
            ", ".join(_missing_dcat_mandatory_fields(config)),
        )
        _dcat_misconfig_warned = True

    if NETWORK:
        network_catalog = await harvest_with_networks(connection)

        if STAC_TRANSFORMER:
            stac_dict = build_stac_catalog_with_networks(network_catalog)
            write_stac_catalog_with_networks(stac_dict)

        if dcat_ready:
            try:
                dcat_dict = build_dcat_catalog_with_networks(network_catalog)
                write_dcat_catalog_with_networks(dcat_dict)
                logger.info(
                    "DCAT cache written: %d Networks, harvested at %s",
                    len(network_catalog.networks), network_catalog.harvested_at,
                )
            except Exception:
                logger.exception(
                    "DCAT transform/write failed this cycle -- STAC write above is "
                    "unaffected, previous DCAT cache left untouched"
                )
    else:
        catalog = await harvest(connection)

        if STAC_TRANSFORMER:
            stac_dict = build_stac_catalog(catalog)
            write_stac_catalog(stac_dict)

        if dcat_ready:
            try:
                dcat_dict = build_dcat_catalog(catalog)
                write_dcat_catalog(dcat_dict)
                logger.info("DCAT cache written: %d Things", catalog.thing_count)
            except Exception:
                logger.exception(
                    "DCAT transform/write failed this cycle -- STAC write above is "
                    "unaffected, previous DCAT cache left untouched"
                )
