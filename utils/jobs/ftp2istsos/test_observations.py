"""Self-check: python test_observations.py (or pytest)."""

from types import SimpleNamespace

import requests
from istsos4_client import TimeInterval

from ftp2istsos4.observations import (
    Target,
    datastream_phenomenon_time_end,
    is_duplicate_observation_error,
    patch_observation,
    post_bulk_observations,
    post_observation,
    record_observation_times,
    to_observation,
)

PAYLOAD = {
    "Datastream": {"@iot.id": 3},
    "phenomenonTime": "2026-01-01T00:00:00+00:00",
    "result": 1.5,
    "resultQuality": "3",
}


def http_error(status, text):
    return requests.HTTPError(
        text, response=SimpleNamespace(status_code=status, text=text)
    )


def test_duplicate_errors_are_recognised():
    assert is_duplicate_observation_error(
        http_error(
            400,
            "duplicate key value violates unique constraint "
            '"unique_observation_phenomenontime_datastreamid"',
        )
    )
    assert is_duplicate_observation_error(
        http_error(409, "Observation already exists")
    )


def test_other_errors_are_not_duplicates():
    assert not is_duplicate_observation_error(http_error(400, "bad payload"))
    assert not is_duplicate_observation_error(http_error(500, "boom"))
    assert not is_duplicate_observation_error(
        requests.HTTPError("no response attached", response=None)
    )


def test_result_time_defaults_to_phenomenon_time():
    assert to_observation(PAYLOAD).serialize() == {
        "phenomenonTime": "2026-01-01T00:00:00Z",
        "result": 1.5,
        "resultTime": "2026-01-01T00:00:00Z",
        "resultQuality": "3",
        "Datastream": {"@iot.id": 3},
    }


def test_recording_posted_times_advances_the_cached_range():
    """Append mode filters on the cached range, so a posted observation has to
    move it or the same rows get re-sent on the next file."""
    target = Target(
        api=None,
        by_name={"ds": 3},
        by_id={3: SimpleNamespace(phenomenon_time=None)},
    )

    record_observation_times(target, [PAYLOAD])
    first = datastream_phenomenon_time_end(target, 3)
    assert first.isoformat() == "2026-01-01T00:00:00+00:00"

    record_observation_times(
        target, [{**PAYLOAD, "phenomenonTime": "2026-01-02T00:00:00+00:00"}]
    )
    assert datastream_phenomenon_time_end(target, 3) > first
    # an older observation must not pull the range backwards
    record_observation_times(
        target, [{**PAYLOAD, "phenomenonTime": "2025-01-01T00:00:00+00:00"}]
    )
    assert datastream_phenomenon_time_end(target, 3).year == 2026
    assert isinstance(target.by_id[3].phenomenon_time, TimeInterval)


def test_dry_run_never_calls_the_api():
    # the stub api only knows base_url, so any HTTP call would raise
    target = Target(api=SimpleNamespace(base_url="http://x"), dry_run=True)
    assert post_observation(target, PAYLOAD) == 201
    assert patch_observation(target, 1, PAYLOAD) == 201
    assert post_bulk_observations(target, [PAYLOAD]) == 201


if __name__ == "__main__":
    for name, check in sorted(list(globals().items())):
        if name.startswith("test_"):
            check()
    print("ok")
