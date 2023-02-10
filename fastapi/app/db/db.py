import asyncpg

pgpool: asyncpg.Pool | None = None

async def get_pool():
    global pgpool
    if not pgpool:
        pgpool = await asyncpg.create_pool(dsn='postgresql://admin:admin@database:5432/istsos')
    return pgpool