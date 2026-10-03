"""
Covers the catch_errors decorator / error_response envelope introduced in
utils.py to replace api.py's ~15 repeated try/except blocks. test_auth.py
and test_dcat_gate_parity.py already exercise the happy/401/404 paths
through the real router; this file is specifically about "the handler
raised" -> 400 with the standard envelope, which nothing else covered.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.v1.api import v1
from app.v1.connector.utils import catch_errors, error_response


@pytest.mark.asyncio
async def test_catch_errors_turns_exception_into_400_envelope():
    @catch_errors
    async def handler():
        raise ValueError("boom")

    response = await handler()

    assert response.status_code == 400
    import json
    body = json.loads(response.body)
    assert body == {"code": 400, "type": "error", "message": "boom"}


@pytest.mark.asyncio
async def test_catch_errors_passes_through_normal_return():
    @catch_errors
    async def handler():
        return {"ok": True}

    assert await handler() == {"ok": True}


def test_error_response_shape():
    response = error_response(404, "Thing not found.")
    assert response.status_code == 404
    import json
    assert json.loads(response.body) == {
        "code": 404, "type": "error", "message": "Thing not found.",
    }


@pytest.mark.asyncio
async def test_stac_root_route_returns_400_envelope_on_cache_failure(monkeypatch):
    """End-to-end: a real cache-layer exception surfacing through the
    mounted router still comes back as the standard 400 envelope, not an
    unhandled 500."""
    monkeypatch.setattr(
        "app.v1.connector.api.get_catalog",
        AsyncMock(side_effect=RuntimeError("redis unavailable")),
    )
    monkeypatch.setattr("app.v1.connector.auth_gate.AUTHORIZATION", 0)

    async with AsyncClient(transport=ASGITransport(app=v1), base_url="http://test") as ac:
        res = await ac.get("/connector/stac")

    assert res.status_code == 400
    assert res.json() == {"code": 400, "type": "error", "message": "redis unavailable"}
