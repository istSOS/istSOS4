"""
conftest.py — shared session-scoped fixtures for the c01 Sensing Core package.

Root fixtures (client, seed, unique_name, base_url) are auto-inherited from
tests/conformance/conftest.py.  The three fixtures below derive single ids
from the seed dataset; making them session-scoped avoids repeated HTTP calls
and is safe because `seed` is already session-scoped.
"""
from __future__ import annotations

import pytest

from client import entity_id, format_id


@pytest.fixture(scope="session")
def hl_id(client, seed):
    """First HistoricalLocation id linked to the seed Thing (navigated at runtime).

    The server MUST create a HistoricalLocation when a Thing is linked to a
    Location (18-088 §8.2.2).
    """
    hl_list = client.values(
        f"Things({format_id(seed.thing_id)})/HistoricalLocations"
    )
    assert hl_list, (
        "seed Thing has no HistoricalLocations; "
        "server MUST create one when a Thing is linked to a Location"
    )
    return entity_id(hl_list[0])


@pytest.fixture(scope="session")
def obs_id(seed):
    """First Observation id from DS1 (results [3, 4])."""
    assert seed.ds1.observation_ids, "seed DS1 has no observations"
    return seed.ds1.observation_ids[0]


@pytest.fixture(scope="session")
def foi_id(seed):
    """First FeatureOfInterest id from the seed dataset."""
    assert seed.foi_ids, "seed has no FeaturesOfInterest"
    return seed.foi_ids[0]
