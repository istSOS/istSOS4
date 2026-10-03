"""
Unit tests for the CATALOG_CLOSED_NETWORKS name-resolution layer.

_parse_closed_network_tokens/_ClosedNetworksRegistry/resolve_closed_networks
are exercised directly here rather than through the router (see test_auth.py
for the end-to-end 401/404 gate behavior) -- this suite is only concerned
with "do names turn into the right ids", not with what the gate does with
the resulting set.
"""

from __future__ import annotations

import logging

import pytest

from app.v1.connector import config


class _FakePool:
    """Minimal stand-in for asyncpg.Pool -- only .fetch() is needed here."""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.calls: list[tuple] = []

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        return self._rows


def test_parse_closed_network_tokens_splits_ids_and_names(monkeypatch):
    monkeypatch.setenv("CATALOG_CLOSED_NETWORKS", " 3, Rain Gauges ,7,Snow Stations ")
    ids, names = config._parse_closed_network_tokens()
    assert ids == frozenset({3, 7})
    assert names == frozenset({"Rain Gauges", "Snow Stations"})


def test_parse_closed_network_tokens_empty(monkeypatch):
    monkeypatch.setenv("CATALOG_CLOSED_NETWORKS", "")
    ids, names = config._parse_closed_network_tokens()
    assert ids == frozenset()
    assert names == frozenset()


def test_registry_is_mutable_and_shared_by_reference():
    """The whole point of _ClosedNetworksRegistry: updating .set() on one
    reference is visible through every other reference to the same object,
    unlike `from config import CATALOG_CLOSED_NETWORKS` rebinding a name."""
    registry = config._ClosedNetworksRegistry(frozenset({1}))
    alias = registry  # stands in for another module's `from ... import CATALOG_CLOSED_NETWORKS`

    assert 1 in alias
    assert 2 not in alias

    registry.set(frozenset({2, 3}))

    assert 1 not in alias
    assert 2 in alias
    assert 3 in alias


@pytest.mark.asyncio
async def test_resolve_closed_networks_noop_when_no_names(monkeypatch):
    """Pure-id CATALOG_CLOSED_NETWORKS (the common case) never touches the DB."""
    monkeypatch.setattr(config, "_CLOSED_NETWORK_NAMES", frozenset())
    monkeypatch.setattr(config, "_CLOSED_NETWORK_LITERAL_IDS", frozenset({5}))
    monkeypatch.setattr(config, "CATALOG_CLOSED_NETWORKS", config._ClosedNetworksRegistry(frozenset({5})))

    pool = _FakePool(rows=[])
    await config.resolve_closed_networks(pool)

    assert pool.calls == []
    assert 5 in config.CATALOG_CLOSED_NETWORKS


@pytest.mark.asyncio
async def test_resolve_closed_networks_resolves_names_to_ids(monkeypatch):
    monkeypatch.setattr(config, "_CLOSED_NETWORK_NAMES", frozenset({"Rain Gauges", "Snow Stations"}))
    monkeypatch.setattr(config, "_CLOSED_NETWORK_LITERAL_IDS", frozenset({3}))
    monkeypatch.setattr(config, "CATALOG_CLOSED_NETWORKS", config._ClosedNetworksRegistry(frozenset({3})))

    pool = _FakePool(rows=[
        {"id": 10, "name": "Rain Gauges"},
        {"id": 11, "name": "Snow Stations"},
    ])
    await config.resolve_closed_networks(pool)

    assert 3 in config.CATALOG_CLOSED_NETWORKS
    assert 10 in config.CATALOG_CLOSED_NETWORKS
    assert 11 in config.CATALOG_CLOSED_NETWORKS
    assert len(pool.calls) == 1
    query, args = pool.calls[0]
    assert "sensorthings" in query
    assert sorted(args[0]) == ["Rain Gauges", "Snow Stations"]


@pytest.mark.asyncio
async def test_resolve_closed_networks_warns_on_unmatched_name(monkeypatch, caplog):
    monkeypatch.setattr(config, "_CLOSED_NETWORK_NAMES", frozenset({"Typo Network"}))
    monkeypatch.setattr(config, "_CLOSED_NETWORK_LITERAL_IDS", frozenset())
    monkeypatch.setattr(config, "CATALOG_CLOSED_NETWORKS", config._ClosedNetworksRegistry(frozenset()))

    pool = _FakePool(rows=[])
    with caplog.at_level(logging.WARNING):
        await config.resolve_closed_networks(pool)

    assert len(config.CATALOG_CLOSED_NETWORKS) == 0
    assert any("Typo Network" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_resolve_closed_networks_db_failure_leaves_ids_untouched(monkeypatch, caplog):
    """If the lookup query itself fails, literal ids already in the
    registry are left alone rather than being wiped out."""
    monkeypatch.setattr(config, "_CLOSED_NETWORK_NAMES", frozenset({"Rain Gauges"}))
    monkeypatch.setattr(config, "_CLOSED_NETWORK_LITERAL_IDS", frozenset({3}))
    monkeypatch.setattr(config, "CATALOG_CLOSED_NETWORKS", config._ClosedNetworksRegistry(frozenset({3})))

    class _BrokenPool:
        async def fetch(self, query, *args):
            raise RuntimeError("connection reset")

    with caplog.at_level(logging.ERROR):
        await config.resolve_closed_networks(_BrokenPool())

    assert 3 in config.CATALOG_CLOSED_NETWORKS
    assert len(config.CATALOG_CLOSED_NETWORKS) == 1
    assert any("failed to resolve" in rec.message for rec in caplog.records)
