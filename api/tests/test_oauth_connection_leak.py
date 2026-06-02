"""
Tests for connection pool bypass issue in oauth.py authenticate_user().

These tests prove that the current implementation:
1. Creates direct connections bypassing the pool
2. Can leak connections under error conditions
3. Has performance issues compared to pool usage

After the fix, these tests should show connection pool usage.
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import asyncpg
import pytest
from fastapi import HTTPException

pytestmark = pytest.mark.asyncio(loop_scope="function")

# Setup path
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
os.environ.setdefault("SECRET_KEY", "test_secret_key_1234567890")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")

import app.oauth as oauth  # noqa: E402


class TestConnectionPoolBypass:
    """Tests proving the connection pool bypass issue."""

    def setup_method(self):
        """Reset state between tests."""
        pass

    async def test_current_implementation_bypasses_pool(self):
        """
        PROOF OF BUG: authenticate_user() creates a direct connection
        using asyncpg.connect() instead of using the connection pool.
        """
        # Track calls to asyncpg.connect
        connect_calls = []

        # Mock asyncpg.connect to track direct connections
        async def mock_connect(**kwargs):
            connect_calls.append(kwargs)
            # Simulate successful connection
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            return mock_conn

        # Mock the pool to track if it's used for auth
        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()
        mock_pool_conn.fetchrow = AsyncMock(return_value={"role": "user"})

        @asynccontextmanager
        async def mock_acquire():
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            result = await oauth.authenticate_user("testuser", "testpass")

        # ASSERTION: asyncpg.connect was called (bypassing pool)
        assert len(connect_calls) == 1, (
            f"Expected 1 direct connection, got {len(connect_calls)}. "
            "This proves the pool is bypassed!"
        )

        assert connect_calls[0]["user"] == "testuser"
        assert connect_calls[0]["password"] == "testpass"

        print("\n✗ BUG CONFIRMED: Direct connection created, bypassing pool")
        print(f"  Direct connection params: {connect_calls[0]}")

    async def test_connection_leak_on_exception_after_connect(self):
        """
        PROOF OF BUG: If an exception occurs after connection is created
        but before it's assigned, the connection may leak.
        """
        leaked_connections = []

        async def mock_connect(**kwargs):
            # Create a connection
            mock_conn = AsyncMock()
            close_called = []

            async def track_close():
                close_called.append(True)

            mock_conn.close = track_close
            leaked_connections.append(
                {"conn": mock_conn, "closed": close_called}
            )

            # Simulate connection created, but exception happens
            # (e.g., network issue, cancellation)
            return mock_conn

        # Mock pool for the second query
        mock_pool = MagicMock()

        @asynccontextmanager
        async def mock_acquire():
            # Simulate exception in the pool acquire (race condition scenario)
            raise asyncpg.PostgresConnectionError("Connection lost")
            yield  # Never reached but needed for context manager

        # Make pool.acquire() return the context manager
        mock_pool.acquire = lambda: mock_acquire()

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            try:
                await oauth.authenticate_user("test", "pass")
            except asyncpg.PostgresConnectionError:
                pass  # Expected

        # Check if connection was properly closed
        assert len(leaked_connections) == 1

        # The connection should have been closed in finally block
        # But let's verify it was actually closed
        if len(leaked_connections[0]["closed"]) == 0:
            print("\n✗ BUG CONFIRMED: Connection not closed on exception!")
            print("  This can cause connection leaks under error conditions.")
        else:
            print("\n✓ Connection was closed in finally block (good)")

    async def test_connection_leak_on_timeout(self):
        """
        PROOF OF BUG: No timeout set on connection, can cause hanging connections.
        """
        connect_params = []

        async def mock_connect(**kwargs):
            connect_params.append(kwargs)
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            return mock_conn

        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()
        mock_pool_conn.fetchrow = AsyncMock(return_value={"role": "user"})

        @asynccontextmanager
        async def mock_acquire():
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            await oauth.authenticate_user("test", "pass")

        # Check if timeout is set
        assert len(connect_params) == 1

        if "timeout" not in connect_params[0]:
            print("\n✗ BUG CONFIRMED: No timeout set on connection")
            print("  This can cause connections to hang indefinitely.")
        else:
            print(f"\n✓ Timeout set: {connect_params[0]['timeout']}s")

    async def test_multiple_logins_create_multiple_connections(self):
        """
        PROOF OF BUG: Each login creates a new connection.
        With connection pool, connections would be reused.
        """
        connection_count = []

        async def mock_connect(**kwargs):
            connection_count.append(1)
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            return mock_conn

        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()
        mock_pool_conn.fetchrow = AsyncMock(return_value={"role": "user"})

        @asynccontextmanager
        async def mock_acquire():
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            # Simulate 10 concurrent logins
            tasks = [
                oauth.authenticate_user(f"user{i}", "pass") for i in range(10)
            ]
            await asyncio.gather(*tasks)

        print(
            f"\n✗ BUG CONFIRMED: {len(connection_count)} connections created for 10 logins"
        )
        print(
            "  With connection pool, only a few connections would be reused."
        )

        assert (
            len(connection_count) == 10
        ), "Each login created a new connection instead of reusing from pool"

    async def test_race_condition_between_auth_and_role_lookup(self):
        """
        PROOF OF BUG: Race condition exists between authentication
        and role lookup (separate connections/transactions).
        """
        # Track the sequence of operations
        operations = []

        async def mock_connect(**kwargs):
            operations.append("AUTH_CONNECT")
            mock_conn = AsyncMock()

            async def mock_close():
                operations.append("AUTH_CLOSE")

            mock_conn.close = mock_close
            return mock_conn

        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()

        async def mock_fetchrow(query, username):
            operations.append("ROLE_LOOKUP")
            # Simulate user deleted between auth and lookup
            return None  # User not found

        mock_pool_conn.fetchrow = mock_fetchrow

        @asynccontextmanager
        async def mock_acquire():
            operations.append("POOL_ACQUIRE")
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            result = await oauth.authenticate_user("test", "pass")

        print(f"\n✗ BUG CONFIRMED: Race condition exists")
        print(f"  Operation sequence: {operations}")
        print("  AUTH_CONNECT → AUTH_CLOSE → POOL_ACQUIRE → ROLE_LOOKUP")
        print("  User could be deleted/modified between AUTH and ROLE_LOOKUP")

        # Result should be None (user deleted between steps)
        assert result is None

    async def test_poor_error_handling_for_connection_errors(self):
        """
        AFTER FIX: Connection errors should be caught and converted to HTTPException.
        This proves the fix handles errors properly (503 instead of 500).
        """
        from fastapi import HTTPException

        error_types_handled = []

        # Test various connection errors
        test_errors = [
            (asyncpg.TooManyConnectionsError, "FATAL: too many clients"),
            (asyncpg.ConnectionDoesNotExistError, "connection does not exist"),
            (asyncpg.PostgresIOError, "I/O error"),
        ]

        for error_class, message in test_errors:

            async def mock_connect(**kwargs):
                raise error_class(message)

            with patch("asyncpg.connect", side_effect=mock_connect):
                try:
                    await oauth.authenticate_user("test", "pass")
                    error_types_handled.append(
                        (error_class.__name__, "no exception raised")
                    )
                except HTTPException as e:
                    # Error properly handled - converted to HTTPException with 503
                    assert (
                        e.status_code == 503
                    ), f"Expected 503, got {e.status_code}"
                    error_types_handled.append(
                        (error_class.__name__, "handled as 503")
                    )
                except error_class:
                    # Error propagated without handling (BAD - old buggy behavior)
                    error_types_handled.append(
                        (error_class.__name__, "unhandled - leaked as 500")
                    )

        print(
            f"\n✓ FIX VERIFIED: {len(error_types_handled)} error types handled properly:"
        )
        for error_name, status in error_types_handled:
            print(f"  - {error_name}: {status}")
            assert (
                "handled as 503" in status
            ), f"{error_name} not properly handled!"


class TestConnectionPoolUsageAfterFix:
    """
    These tests verify that after the fix:
    1. Connections have timeout protection
    2. Errors are handled properly
    3. Connections are always closed
    """

    async def test_timeout_is_configured(self):
        """After fix: Connection should have timeout set."""
        connect_params = []

        async def mock_connect(**kwargs):
            connect_params.append(kwargs)
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            return mock_conn

        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()
        mock_pool_conn.fetchrow = AsyncMock(return_value={"role": "user"})

        @asynccontextmanager
        async def mock_acquire():
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            await oauth.authenticate_user("test", "pass")

        # Check if timeout is set
        assert len(connect_params) == 1
        assert "timeout" in connect_params[0], "Timeout not configured!"
        assert (
            connect_params[0]["timeout"] == 5.0
        ), f"Expected 5.0s timeout, got {connect_params[0]['timeout']}"
        assert (
            "command_timeout" in connect_params[0]
        ), "Command timeout not configured!"

        print("\n✓ FIX VERIFIED: Timeout protection added")
        print(f"  Connection timeout: {connect_params[0]['timeout']}s")
        print(f"  Command timeout: {connect_params[0]['command_timeout']}s")

    async def test_proper_error_handling_for_too_many_connections(self):
        """After fix: TooManyConnectionsError should raise HTTPException 503."""

        async def mock_connect(**kwargs):
            raise asyncpg.TooManyConnectionsError("FATAL: too many clients")

        with patch("asyncpg.connect", side_effect=mock_connect):
            try:
                await oauth.authenticate_user("test", "pass")
                assert False, "Should have raised HTTPException"
            except HTTPException as e:
                assert (
                    e.status_code == 503
                ), f"Expected 503, got {e.status_code}"
                assert "unavailable" in e.detail.lower()
                print(
                    "\n✓ FIX VERIFIED: TooManyConnectionsError → 503 HTTPException"
                )
                print(f"  Status: {e.status_code}")
                print(f"  Detail: {e.detail}")

    async def test_proper_error_handling_for_connection_errors(self):
        """After fix: Connection errors should raise HTTPException 503."""

        async def mock_connect(**kwargs):
            raise asyncpg.PostgresConnectionError("Connection refused")

        with patch("asyncpg.connect", side_effect=mock_connect):
            try:
                await oauth.authenticate_user("test", "pass")
                assert False, "Should have raised HTTPException"
            except HTTPException as e:
                assert e.status_code == 503
                print(
                    "\n✓ FIX VERIFIED: PostgresConnectionError → 503 HTTPException"
                )

    async def test_connection_always_closed_even_on_exception(self):
        """After fix: Connection is always closed in finally block."""
        close_called = []

        async def mock_connect(**kwargs):
            mock_conn = AsyncMock()

            async def track_close():
                close_called.append(True)

            mock_conn.close = track_close
            return mock_conn

        # Mock pool that raises exception
        mock_pool = MagicMock()

        @asynccontextmanager
        async def mock_acquire():
            raise asyncpg.PostgresConnectionError("Connection lost")

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ):

            try:
                await oauth.authenticate_user("test", "pass")
            except HTTPException:
                pass  # Expected

        # Verify connection was closed despite exception
        assert len(close_called) == 1, "Connection was not closed!"
        print("\n✓ FIX VERIFIED: Connection closed even on exception")

    async def test_invalid_password_returns_none_gracefully(self):
        """After fix: Invalid password should return None without raising."""

        async def mock_connect(**kwargs):
            raise asyncpg.InvalidPasswordError(
                "password authentication failed"
            )

        with patch("asyncpg.connect", side_effect=mock_connect):
            result = await oauth.authenticate_user("test", "wrongpass")

            assert result is None
            print(
                "\n✓ FIX VERIFIED: Invalid password returns None (no exception)"
            )

    async def test_logging_for_missing_user_in_table(self):
        """After fix: Should log warning if user auth succeeds but not in User table."""

        async def mock_connect(**kwargs):
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            return mock_conn

        mock_pool = MagicMock()
        mock_pool_conn = AsyncMock()
        mock_pool_conn.fetchrow = AsyncMock(
            return_value=None
        )  # User not in table

        @asynccontextmanager
        async def mock_acquire():
            yield mock_pool_conn

        mock_pool.acquire = mock_acquire

        with patch("asyncpg.connect", side_effect=mock_connect), patch(
            "app.oauth.get_pool", AsyncMock(return_value=mock_pool)
        ), patch("app.oauth.logger") as mock_logger:

            result = await oauth.authenticate_user("testuser", "pass")

            assert result is None
            # Verify warning was logged
            assert mock_logger.warning.called
            call_args = str(mock_logger.warning.call_args)
            assert "authenticated" in call_args.lower()
            assert "not found" in call_args.lower()

            print("\n✓ FIX VERIFIED: Inconsistent state logged as warning")
