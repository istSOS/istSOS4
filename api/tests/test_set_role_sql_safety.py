import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="function")

# Ensure api/ is on sys.path so 'app' resolves to api/app
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Patch env vars before importing app
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")

from app.v1.endpoints.functions import set_role  # noqa: E402


class DummyConnection:
    def __init__(self):
        self.execute = AsyncMock()

    @asynccontextmanager
    async def transaction(self):
        yield


def test_set_role_quotes_valid_identifier():
    conn = DummyConnection()
    current_user = {"username": "test_user"}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET ROLE "test_user";')


def test_set_role_rejects_invalid_identifier():
    conn = DummyConnection()
    current_user = {"username": 'bad"name'}

    with pytest.raises(ValueError, match="Invalid role identifier"):
        asyncio.run(set_role(conn, current_user))

    conn.execute.assert_not_awaited()