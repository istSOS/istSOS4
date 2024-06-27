import os

import asyncpg

pgpool: asyncpg.Pool | None = None


async def get_pool():
    global pgpool
    if not pgpool:
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        dsn = f"postgresql://{user}:{password}@database:5432/{db}"
        pgpool = await asyncpg.create_pool(dsn=dsn)
    return pgpool
