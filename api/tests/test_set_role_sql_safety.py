import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="function")

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")

from app.v1.endpoints.functions import set_role  # noqa: E402


async def test_set_role_quotes_postgres_identifier():
    connection = AsyncMock()

    @asynccontextmanager
    async def transaction():
        yield

    connection.transaction = transaction

    await set_role(connection, {"username": 'test"user'})

    connection.execute.assert_awaited_once_with('SET ROLE "test""user";')
