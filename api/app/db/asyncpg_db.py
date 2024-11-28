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
