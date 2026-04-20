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

import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
from app import HOSTNAME, POSTGRES_PORT_WRITE, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool, get_pool_w
from app.settings import serverSettings, tables
from app.v1 import api
from fastapi import FastAPI


logger = logging.getLogger(__name__)


async def initialize_pool():
    retries = 0
    max_retries = 30

    while True:
        try:
            await get_pool()  # Ensure get_pool() is awaited
            if POSTGRES_PORT_WRITE:
                await get_pool_w()
            break
        except (
            asyncpg.PostgresConnectionError,
            asyncpg.TooManyConnectionsError,
        ) as error:
            retries += 1
            logger.warning(
                "Database pool initialization failed (attempt %s/%s): %s",
                retries,
                max_retries,
                error,
            )
            if retries >= max_retries:
                raise
            await asyncio.sleep(1)  # Use asyncio.sleep for asynchronous sleep
        except ValueError:
            logger.exception("Invalid database configuration during startup")
            raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_pool()
    yield


app = FastAPI(
    title="OGC SensorThings API",
    description="A SensorThings API implementation in Python using FastAPI.",
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "Read root",
        }
    ],
)


def __handle_root():
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": f"{HOSTNAME}{SUBPATH}{VERSION}" + "/" + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    }
    return response

@app.get(f"{SUBPATH}{VERSION}", tags=["Read root"])
async def read_root():
    return __handle_root()


app.mount(f"{SUBPATH}{VERSION}", api.v1)
