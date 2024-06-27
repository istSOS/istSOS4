import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(
    os.getenv("DATABASE_URL"), pool_size=20, max_overflow=0
)

# Create the asynchronous session
SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
)

Base = declarative_base()

SCHEMA_NAME = "sensorthings"


def get_db():
    return SessionLocal
