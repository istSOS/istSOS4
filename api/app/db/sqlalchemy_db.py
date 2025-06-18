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

from app import (
    ISTSOS_ADMIN,
    ISTSOS_ADMIN_PASSWORD,
    PG_MAX_OVERFLOW,
    PG_POOL_SIZE,
    PG_POOL_TIMEOUT,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base

dsn = f"postgresql+asyncpg://{ISTSOS_ADMIN}:{ISTSOS_ADMIN_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(
    dsn,
    pool_size=PG_POOL_SIZE,
    max_overflow=PG_MAX_OVERFLOW,
    pool_timeout=PG_POOL_TIMEOUT,
    pool_recycle=3600,
    pool_pre_ping=True,
)

Base = declarative_base()

SCHEMA_NAME = "sensorthings"
