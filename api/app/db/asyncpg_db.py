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

import asyncpg
from app import (
    ISTSOS_ADMIN,
    ISTSOS_ADMIN_PASSWORD,
    PG_POOL_SIZE,
    PG_POOL_TIMEOUT,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_PORT_WRITE,
)

pgpool: asyncpg.Pool | None = None

if POSTGRES_PORT_WRITE:
    pgpoolw: asyncpg.Pool | None = None


async def get_pool():
    global pgpool
    if not pgpool:
        dsn = f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

        pgpool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=PG_POOL_SIZE,
            max_size=PG_POOL_SIZE,
            timeout=PG_POOL_TIMEOUT,
            max_queries=50000,
            max_inactive_connection_lifetime=3600,
        )
    return pgpool


async def get_pool_w():
    global pgpoolw
    if not pgpoolw:
        dsn = f"postgresql://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT_WRITE}/{POSTGRES_DB}"

        pgpoolw = await asyncpg.create_pool(
            dsn=dsn,
            min_size=PG_POOL_SIZE,
            max_size=PG_POOL_SIZE,
            timeout=PG_POOL_TIMEOUT,
            max_queries=50000,
            max_inactive_connection_lifetime=3600,
        )
    return pgpoolw
