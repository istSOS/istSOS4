import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

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


def test_set_role_escapes_identifier_quotes():
    conn = DummyConnection()
    current_user = {"username": 'bad"name'}

    asyncio.run(set_role(conn, current_user))

    conn.execute.assert_awaited_once_with('SET ROLE "bad""name";')
