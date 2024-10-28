import asyncpg
from app import (
    PG_POOL_SIZE,
    PG_POOL_TIMEOUT,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

pgpool: asyncpg.Pool | None = None


async def get_pool():
    global pgpool
    if not pgpool:
        dsn = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

        pgpool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=PG_POOL_SIZE,
            max_size=PG_POOL_SIZE,
            timeout=PG_POOL_TIMEOUT,
            max_queries=50000,
            max_inactive_connection_lifetime=3600,
        )
    return pgpool
