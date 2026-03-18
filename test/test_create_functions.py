"""
Tests for set_commit() in api/app/v1/endpoints/create/functions.py

Covers all branching logic before the DB call, no live database needed.

Author: Vishmayraj
"""

import sys
import os
import pytest
from unittest.mock import AsyncMock, patch

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
API_DIR = os.path.join(PROJECT_ROOT, "api")

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, API_DIR)

from app.v1.endpoints.create.functions import set_commit

MODULE = "app.v1.endpoints.create.functions"


def make_connection(fetchval_return=None):
    """Async mock with awaitable execute and fetchval, matching the asyncpg connection contract."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=fetchval_return)
    return conn


class TestSetCommitDisabled:
    """Both VERSIONING and AUTHORIZATION are False, return None immediately, no DB touched."""

    @pytest.mark.asyncio
    async def test_returns_none_when_both_flags_off(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            result = await set_commit(conn, "some message", None)
        assert result is None

    @pytest.mark.asyncio
    async def test_does_not_call_execute_when_disabled(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            await set_commit(conn, "msg", {"role": "admin", "id": 1})
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_call_fetchval_when_disabled(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            await set_commit(conn, "msg", {"role": "admin", "id": 1})
        conn.fetchval.assert_not_called()


class TestSetCommitSensorRole:
    """Sensor role: forbidden to provide a commit message; without one, fetches their existing commit id."""

    @pytest.mark.asyncio
    async def test_sensor_with_message_raises(self):
        conn = make_connection()
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="Sensor cannot provide commit message"):
                await set_commit(conn, "should not be here", sensor)

    @pytest.mark.asyncio
    async def test_sensor_with_message_resets_role_before_raising(self):
        """RESET ROLE must be called before the exception propagates."""
        conn = make_connection()
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception):
                await set_commit(conn, "bad message", sensor)
        conn.execute.assert_called_once_with("RESET ROLE;")

    @pytest.mark.asyncio
    async def test_sensor_without_message_fetches_existing_commit(self):
        conn = make_connection(fetchval_return=99)
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            result = await set_commit(conn, None, sensor)
        assert result == 99

    @pytest.mark.asyncio
    async def test_sensor_without_message_calls_fetchval_once(self):
        conn = make_connection(fetchval_return=99)
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            await set_commit(conn, None, sensor)
        conn.fetchval.assert_called_once()


class TestSetCommitNoMessage:
    """Non-sensor user with no commit message, RESET ROLE then raise, same as the sensor message violation."""

    @pytest.mark.asyncio
    async def test_no_message_raises(self):
        conn = make_connection()
        admin = {"role": "administrator", "id": 1, "uri": "http://example.com/users/1"}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="No commit message provided"):
                await set_commit(conn, None, admin)

    @pytest.mark.asyncio
    async def test_no_message_resets_role_before_raising(self):
        conn = make_connection()
        admin = {"role": "administrator", "id": 1, "uri": "http://example.com/users/1"}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception):
                await set_commit(conn, None, admin)
        conn.execute.assert_called_once_with("RESET ROLE;")

    @pytest.mark.asyncio
    async def test_anonymous_user_no_message_raises(self):
        """current_user=None with no message must also raise, not crash."""
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="No commit message provided"):
                await set_commit(conn, None, None)
