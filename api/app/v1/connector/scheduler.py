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

Cache writes: stac and dcat are written within one cycle, or neither is.
A failure before both writes complete leaves the previous valid Redis
cache untouched.

Public interface:
    start_scheduler(pool) -> AsyncIOScheduler
"""

from __future__ import annotations

import logging
from datetime import datetime

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.v1.connector.cache import write_stac_catalog
from app.v1.connector.config import get_settings
from app.v1.connector.harvester import harvest
from app.v1.connector.stac_transformer import build_stac_catalog

logger = logging.getLogger(__name__)

_HARVEST_LOCK_KEY = 726419950_1

try:
    # Both currently not implemented
    from app.v1.connector.dcat_transformer import build_dcat_catalog
    from app.v1.connector.cache import write_dcat_catalog
except ImportError:
    build_dcat_catalog = None
    write_dcat_catalog = None


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

    STAC is always written. DCAT is written only when dcat_transformer.py
    has been implemented (i.e. build_dcat_catalog is not None). The two
    writes are independent -- a future DCAT failure will not roll back the
    STAC write, and vice versa.
    """
    catalog = await harvest(connection)

    stac_dict = build_stac_catalog(catalog)
    write_stac_catalog(stac_dict)

    if build_dcat_catalog is not None:
        dcat_dict = build_dcat_catalog(catalog, config)
        write_dcat_catalog(dcat_dict)
        logger.info("DCAT cache written: %d Datasets", catalog.thing_count)
    else:
        logger.warning(
            "dcat_transformer.py not implemented yet -- DCAT cache not "
            "written this cycle. STAC cache write above is unaffected."
        )