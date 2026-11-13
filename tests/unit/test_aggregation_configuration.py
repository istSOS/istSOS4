"""Business rules for the configuration contract, independent of a database."""

from datetime import UTC, datetime

import pytest
from app.v1.endpoints import aggregation_configuration as configurations
from app.v1.endpoints.exceptions import BadRequest, Forbidden


def create(**overrides):
    return configurations.validate_create_payload(
        {"datastreamId": 13, "minInputQuality": 3, **overrides}
    )


def test_required_qc_has_no_fallback_and_zero_is_a_valid_choice():
    with pytest.raises(BadRequest, match="minInputQuality.*required"):
        configurations.validate_create_payload({"datastreamId": 13})
    with pytest.raises(BadRequest, match="cannot be null"):
        create(minInputQuality=None)
    assert create(minInputQuality=0)["minInputQuality"] == 0


@pytest.mark.parametrize("quality", [-1, 4, True, 1.0, "3", [], {}])
def test_quality_is_an_automatic_level_not_an_arbitrary_mask(quality):
    with pytest.raises(BadRequest):
        create(minInputQuality=quality)


@pytest.mark.parametrize("identifier", [0, -1, True, "13", 1.5, 2**63])
def test_source_id_is_a_positive_bigint(identifier):
    with pytest.raises(BadRequest):
        create(datastreamId=identifier)


def test_optional_overrides_remain_unset_instead_of_freezing_worker_defaults():
    values = create()
    assert values["enabled"] is True
    assert values["ignoreQuality"] is False
    assert values["mode"] == "sampled"
    assert "intervals" not in values
    assert values["minCoverage"] is None
    assert values["conversionFactor"] is None


@pytest.mark.parametrize(
    "intervals",
    [None, [], ["PT10M"], ["PT15M"]],
)
def test_intervals_are_fixed_and_not_configuration_overrides(intervals):
    with pytest.raises(BadRequest):
        create(intervals=intervals)
    with pytest.raises(BadRequest):
        configurations.validate_patch_payload({"intervals": intervals}, create())


@pytest.mark.parametrize(
    "coverage", [-0.01, 1.01, float("nan"), float("inf"), True, "0.8"]
)
def test_coverage_is_a_finite_fraction(coverage):
    with pytest.raises(BadRequest):
        create(minCoverage=coverage)


@pytest.mark.parametrize(
    "field,value",
    [
        ("enabled", None),
        ("enabled", 1),
        ("mode", None),
        ("mode", "unknown"),
        ("minObservations", -1),
        ("minObservations", True),
        ("maxConsecutiveMissing", -1),
        ("bucketsToRecompute", 0),
        ("utcOffsetMinutes", 841),
        ("boundaryMode", "closed"),
        ("conversionFactor", float("inf")),
        ("conversionFactor", float("nan")),
        ("conversionFactor", 10**1000),
        ("missingValues", [None]),
        ("missingValues", [float("nan")]),
        ("resultQualityKey", "  "),
        ("resultQualityKey", "a" * 256),
    ],
)
def test_invalid_processing_settings_are_rejected(field, value):
    with pytest.raises(BadRequest):
        create(**{field: value})


def test_valid_zero_values_and_empty_missing_marker_list_are_preserved():
    values = create(
        minCoverage=0,
        minObservations=0,
        maxConsecutiveMissing=0,
        utcOffsetMinutes=0,
        conversionFactor=0,
        missingValues=[],
    )
    assert values["minObservations"] == 0
    assert values["conversionFactor"] == 0
    assert values["missingValues"] == []


def test_patch_changes_multiple_settings_and_preserves_omitted_values():
    existing = create(minCoverage=0.8, maxConsecutiveMissing=2)
    updated = configurations.validate_patch_payload(
        {"enabled": False, "minInputQuality": 2, "bucketsToRecompute": 5},
        existing,
    )
    assert updated["enabled"] is False
    assert updated["minInputQuality"] == 2
    assert updated["bucketsToRecompute"] == 5
    assert updated["minCoverage"] == 0.8
    assert updated["maxConsecutiveMissing"] == 2


def test_patch_null_resets_overrides_but_cannot_remove_required_qc():
    existing = create(minCoverage=0.8, conversionFactor=0.2)
    updated = configurations.validate_patch_payload(
        {"minCoverage": None, "conversionFactor": None}, existing
    )
    assert updated["minCoverage"] is None
    assert updated["conversionFactor"] is None
    with pytest.raises(BadRequest):
        configurations.validate_patch_payload({"minInputQuality": None}, existing)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"datastreamId": 2},
        {"id": 1},
        {"createdAt": "2026-01-01"},
        {"scheduleInterval": "1 second"},
        {"targetDatastream": "derived"},
    ],
)
def test_patch_cannot_change_identity_or_old_job_settings(payload):
    with pytest.raises(BadRequest):
        configurations.validate_patch_payload(payload, create())


def test_switching_to_event_mode_requires_clearing_existing_sample_rules():
    existing = create(minCoverage=0.8, minObservations=4, maxConsecutiveMissing=1)
    with pytest.raises(BadRequest, match="completeness"):
        configurations.validate_patch_payload({"mode": "event"}, existing)
    updated = configurations.validate_patch_payload(
        {
            "mode": "event",
            "minCoverage": None,
            "minObservations": None,
            "maxConsecutiveMissing": None,
        },
        existing,
    )
    assert updated["mode"] == "event"


def test_patch_checks_period_against_unchanged_bound():
    existing = {
        "datastreamId": 13,
        "minInputQuality": 3,
        "startTime": "2026-01-01T00:00:00+01:00",
        "endTime": "2026-02-01T00:00:00+01:00",
    }
    with pytest.raises(BadRequest, match="before"):
        configurations.validate_patch_payload(
            {"startTime": "2026-03-01T00:00:00Z"}, existing
        )
    updated = configurations.validate_patch_payload({"endTime": None}, existing)
    assert updated["endTime"] is None
    assert updated["startTime"] == datetime(2025, 12, 31, 23, tzinfo=UTC)


@pytest.mark.parametrize("value", ["2026-01-01", "2026-01-01T00:00:00", "bad", 123])
def test_processing_dates_need_explicit_timezone(value):
    with pytest.raises(BadRequest):
        create(startTime=value)


@pytest.mark.parametrize("role", [None, "viewer", "editor", "obs_manager"])
def test_non_administrators_cannot_manage_configurations(monkeypatch, role):
    monkeypatch.setattr(configurations, "AUTHORIZATION", 1)
    with pytest.raises(Forbidden):
        configurations.require_administrator(None if role is None else {"role": role})
    configurations.require_administrator({"role": "administrator"})


def test_legacy_job_routes_are_not_registered():
    from app.v1.api import v1

    paths = v1.openapi()["paths"]
    assert "/AggregationConfigurations" in paths
    assert set(paths["/AggregationConfigurations({configuration_id})"]) == {
        "get",
        "patch",
        "delete",
    }
    assert not any("AggregationJobs" in path for path in paths)


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
def test_ignore_quality_requires_a_boolean(value):
    with pytest.raises(BadRequest):
        create(ignoreQuality=value)
    with pytest.raises(BadRequest):
        configurations.validate_patch_payload({"ignoreQuality": value}, create())


def test_ignore_quality_can_be_toggled_without_losing_threshold():
    existing = create(minInputQuality=2, ignoreQuality=True)
    assert existing["ignoreQuality"] is True
    assert existing["minInputQuality"] == 2
    updated = configurations.validate_patch_payload({"ignoreQuality": False}, existing)
    assert updated["ignoreQuality"] is False
    assert updated["minInputQuality"] == 2
