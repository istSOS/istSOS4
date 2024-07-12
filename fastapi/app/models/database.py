import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")
host = os.getenv("POSTGRES_HOST", "database")
port = os.getenv("POSTGRES_PORT", "5432")
dsn = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"

engine = create_async_engine(
    dsn,
    pool_size=int(os.getenv("PG_POOL_SIZE", 10)),
    max_overflow=int(os.getenv("PG_MAX_OVERFLOW", 0)),
    pool_timeout=float(os.getenv("PG_POOL_TIMEOUT", 30)),
    pool_recycle=3600,
    pool_pre_ping=True,
)

# Create the asynchronous session
SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
)

Base = declarative_base()

SCHEMA_NAME = "sensorthings"


@asynccontextmanager
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
