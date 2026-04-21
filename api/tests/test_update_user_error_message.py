import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

os.environ["DEBUG"] = "0"
os.environ["AUTHORIZATION"] = "0"
os.environ.setdefault("SECRET_KEY", "test_secret_key")

from app.v1.endpoints.update import user as user_ep  # noqa: E402


class MissingUserRelation(Exception):
    pass


def _pool_with_connection(connection):
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=None)
    connection.transaction = MagicMock(return_value=transaction)

    acquire = MagicMock()
    acquire.__aenter__ = AsyncMock(return_value=connection)
    acquire.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire)
    return pool


@pytest.mark.asyncio
async def test_update_user_undefined_object_message_is_user_not_found():
    connection = MagicMock()
    connection.fetchrow = AsyncMock(side_effect=MissingUserRelation("missing"))
    pool = _pool_with_connection(connection)

    with patch.object(user_ep, "UndefinedObjectError", MissingUserRelation):
        response = await user_ep.update_user(
            user="missing_user",
            payload={"role": "viewer"},
            current_user=None,
            pgpool=pool,
        )

    assert response.status_code == 404
    assert response.body.decode() == '{"message":"User not found"}'
