import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(
    os.getenv("DATABASE_URL"),
    pool_size=os.getenv("PG_POOL_SIZE"),  # Dimensione del pool di connessioni
    max_overflow=os.getenv(
        "PG_MAX_OVERFLOW"
    ),  # Numero di connessioni extra che possono essere create se il pool è pieno
    pool_timeout=os.getenv(
        "PG_POOL_TIMEOUT"
    ),  # Tempo di attesa prima di lanciare un timeout
    pool_recycle=3600,  # Riciclo delle connessioni dopo un'ora
    pool_pre_ping=True,  # Controllo delle connessioni prima di riutilizzarle
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
