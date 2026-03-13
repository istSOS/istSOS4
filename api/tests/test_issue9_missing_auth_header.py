import os
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

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

from app.v1.endpoints.create import login as login_ep


class TestIssue9MissingAuthHeader:
    async def test_refresh_returns_400_when_authorization_missing(self):
        with pytest.raises(HTTPException) as exc_info:
            await login_ep.refresh_token(None)

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Invalid authorization header format"

    async def test_logout_returns_400_when_authorization_missing(self):
        with pytest.raises(HTTPException) as exc_info:
            await login_ep.logout(None)

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Invalid authorization header format"
