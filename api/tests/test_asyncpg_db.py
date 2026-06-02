"""Tests for api/app/db/asyncpg_db.py — get_pool_w guard (Issue 6 fix)."""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure api/ is on sys.path so 'app' resolves to api/app
# ---------------------------------------------------------------------------
API_DIR = str(Path(__file__).resolve().parents[1])  # istSOS4/api/
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Patch env vars BEFORE importing app so __init__.py reads them correctly
os.environ.setdefault("ISTSOS_ADMIN", "admin")
os.environ.setdefault("ISTSOS_ADMIN_PASSWORD", "secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "istsos")
os.environ.setdefault("POSTGRES_USER", "admin")

import app.db.asyncpg_db as asyncpg_db  # noqa: E402  (import after path setup)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetPoolWGuard:
    """Verify the POSTGRES_PORT_WRITE guard added in Issue 6 fix."""

    def setup_method(self):
        """Reset the pool between tests."""
        asyncpg_db.pgpoolw = None
        asyncpg_db.pgpool = None

    def test_raises_valueerror_when_port_write_is_none(self):
        """
        get_pool_w() must raise ValueError with a descriptive message when
        POSTGRES_PORT_WRITE is not configured (None).
        """
        with patch.object(asyncpg_db, "POSTGRES_PORT_WRITE", None):
            with pytest.raises(ValueError) as exc_info:
                asyncio.run(asyncpg_db.get_pool_w())

        assert "POSTGRES_PORT_WRITE" in str(exc_info.value)

    def test_raises_valueerror_when_port_write_is_empty_string(self):
        """
        get_pool_w() must raise ValueError when POSTGRES_PORT_WRITE is an
        empty string (os.getenv returns '' if the var is exported but blank).
        """
        with patch.object(asyncpg_db, "POSTGRES_PORT_WRITE", ""):
            with pytest.raises(ValueError) as exc_info:
                asyncio.run(asyncpg_db.get_pool_w())

        assert "POSTGRES_PORT_WRITE" in str(exc_info.value)

    def test_creates_pool_when_port_write_is_configured(self):
        """
        get_pool_w() must proceed to asyncpg.create_pool when
        POSTGRES_PORT_WRITE is a valid port string.
        """
        fake_pool = MagicMock()
        with patch.object(asyncpg_db, "POSTGRES_PORT_WRITE", "5433"), patch(
            "asyncpg.create_pool", new=AsyncMock(return_value=fake_pool)
        ):
            result = asyncio.run(asyncpg_db.get_pool_w())

        assert result is fake_pool

    def test_dsn_contains_port_write(self):
        """
        When POSTGRES_PORT_WRITE is set, the DSN passed to asyncpg.create_pool
        must contain the write port, not the read port.
        """
        captured_dsn = {}
        fake_pool = MagicMock()

        async def fake_create_pool(dsn, **kwargs):
            captured_dsn["dsn"] = dsn
            return fake_pool

        with patch.object(
            asyncpg_db, "POSTGRES_PORT_WRITE", "5433"
        ), patch.object(asyncpg_db, "POSTGRES_PORT", "5432"), patch(
            "asyncpg.create_pool", new=fake_create_pool
        ):
            asyncio.run(asyncpg_db.get_pool_w())

        assert "5433" in captured_dsn["dsn"], "Write port must appear in DSN"
        assert (
            ":5432/" not in captured_dsn["dsn"]
        ), "Read port must NOT appear in write DSN"

    def test_pgpoolw_always_declared_at_module_level(self):
        """
        pgpoolw must be declared at module level regardless of
        POSTGRES_PORT_WRITE being set or not (fixes the original NameError).
        """
        assert hasattr(
            asyncpg_db, "pgpoolw"
        ), "pgpoolw must be declared unconditionally at module level"
