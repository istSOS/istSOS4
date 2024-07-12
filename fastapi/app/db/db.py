import os

import asyncpg

pgpool: asyncpg.Pool | None = None


async def get_pool():
    global pgpool
    if not pgpool:
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        host = os.getenv("POSTGRES_HOST", "database")
        port = os.getenv("POSTGRES_PORT", "5432")
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"

        pgpool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=int(os.getenv("PG_POOL_SIZE", 10)),
            max_size=int(os.getenv("PG_POOL_SIZE", 10)),
            timeout=float(os.getenv("PG_POOL_TIMEOUT", 30)),
            max_queries=50000,
            max_inactive_connection_lifetime=3600,
        )
    return pgpool
