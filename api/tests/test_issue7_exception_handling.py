import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request

pytestmark = pytest.mark.asyncio(loop_scope="function")

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")

from app import main as app_main
from app.v1.endpoints.create import user as user_ep
from app.v1.endpoints.read import read as read_ep


class TestIssue7ExceptionHandling:
    async def test_create_user_unexpected_error_returns_500_without_details(
        self,
    ):
        pool = MagicMock()
        tx = MagicMock()
        tx.__aenter__ = AsyncMock(return_value=None)
        tx.__aexit__ = AsyncMock(return_value=None)

        conn = MagicMock()
        conn.transaction = MagicMock(return_value=tx)
        conn.fetchrow = AsyncMock(
            side_effect=RuntimeError("db boom details leaked")
        )

        acq = MagicMock()
        acq.__aenter__ = AsyncMock(return_value=conn)
        acq.__aexit__ = AsyncMock(return_value=None)
        pool.acquire = MagicMock(return_value=acq)

        payload = {"username": "u1", "password": "p1", "role": "viewer"}

        with patch.object(user_ep, "set_role", AsyncMock(return_value=None)):
            response = await user_ep.create_user(
                payload=payload,
                current_user={"role": "administrator", "username": "admin"},
                pgpool=pool,
            )

        assert response.status_code == 500
        assert "details leaked" not in response.body.decode()

    async def test_catch_all_get_stream_error_returns_500(self):
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/istsos4/v1.1/Things",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1),
            "server": ("testserver", 80),
            "scheme": "http",
        }
        request = Request(scope)

        async def broken_stream(*args, **kwargs):
            if False:
                yield None
            raise RuntimeError("stream boom")

        with patch.object(
            read_ep.sta2rest.STA2REST,
            "convert_query",
            return_value={
                "main_entity": "Thing",
                "main_query": "SELECT 1",
                "top_value": 1,
                "is_count": False,
                "count_queries": [],
                "as_of_value": None,
                "from_to_value": False,
                "single_result": False,
            },
        ), patch.object(
            read_ep,
            "asyncpg_stream_results",
            side_effect=lambda *a, **k: broken_stream(),
        ):
            response = await read_ep.catch_all_get(
                request=request,
                path_name="Things",
                current_user=None,
                pool=MagicMock(),
                params=None,
            )

        assert response.status_code == 500
        assert response.body.decode() == (
            '{"code":500,"type":"error","message":"Internal server error"}'
        )

    async def test_initialize_pool_valueerror_is_not_retried(self):
        with patch.object(
            app_main,
            "get_pool",
            AsyncMock(side_effect=ValueError("invalid config")),
        ) as get_pool_mock:
            with pytest.raises(ValueError):
                await app_main.initialize_pool()

        assert get_pool_mock.await_count == 1
