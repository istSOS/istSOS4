import json
import signal
from datetime import UTC, datetime, timedelta
from threading import Event

import pytest
import requests

from aggregation import service

BASE = "http://api:5000/istsos4/v1.1"


def configuration(identifier=1, **overrides):
    return {
        "id": identifier,
        "datastreamId": identifier,
        "enabled": True,
        "mode": "sampled",
        "minInputQuality": 3,
        "utcOffsetMinutes": None,
        **overrides,
    }


def datastream(identifier=1):
    return {
        "@iot.id": identifier,
        "name": f"Temperature {identifier}",
        "description": "Test",
        "observationType": "OM_Measurement",
        "unitOfMeasurement": {"name": "Celsius", "symbol": "C", "definition": "test"},
        "properties": {
            "samplingFrequency": "PT10M",
            "constraints": [{"name": "range"}],
        },
    }


@pytest.fixture
def transport(monkeypatch):
    calls = []
    routes = {}

    def request(session, method, url, **kwargs):
        calls.append((method, url, kwargs))
        key = (method, url)
        assert key in routes, f"Unexpected request: {key}"
        response = routes[key]
        if isinstance(response, list):
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        status, body = response
        result = requests.Response()
        result.status_code = status
        result._content = json.dumps(body).encode()
        result.url = url
        result.request = requests.Request(method, url).prepare()
        return result

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr("istsos4_client.client.time.sleep", lambda _: None)
    return routes, calls


def test_client_reads_all_pages_and_datastreams_with_public_nextlink(transport, capsys):
    routes, calls = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (
        200,
        {
            "value": [configuration()],
            "@iot.nextLink": "https://public.example/istsos4/v1.1/AggregationConfigurations?enabled=true&$top=100&$skip=100",
        },
    )
    routes[
        "GET", BASE + "/AggregationConfigurations?enabled=true&$top=100&$skip=100"
    ] = (
        200,
        {
            "value": [
                configuration(2, utcOffsetMinutes=0),
                configuration(3, enabled=False),
            ]
        },
    )
    for identifier in (1, 2):
        routes["GET", BASE + f"/Datastreams({identifier})"] = (
            200,
            datastream(identifier),
        )
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    try:
        assert service.run_cycle(api, settings, api.stop)
    finally:
        api.close()
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [record["datastreamId"] for record in records] == [1, 2]
    assert [record["effectiveUtcOffsetMinutes"] for record in records] == [60, 0]
    assert records[0]["constraintCount"] == 1
    assert records[0]["samplingFrequency"] == "PT10M"
    assert records[0]["configuration"]["minInputQuality"] == 3
    assert not records[0]["warnings"]
    assert all(method == "GET" for method, _, _ in calls)
    assert all(url.startswith(BASE) for _, url, _ in calls)
    assert all(kwargs["timeout"] == 30 for _, _, kwargs in calls)


def test_project_client_handles_login_and_expiry_and_each_cycle(transport, capsys):
    routes, calls = transport
    routes["POST", BASE + "/Login"] = (
        200,
        {"access_token": "private-token", "expires_in": 3600},
    )
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (
        200,
        {"value": [configuration()]},
    )
    routes["GET", BASE + "/Datastreams(1)"] = (200, datastream())
    settings = service.Settings(
        BASE, authorization=True, username="admin", password="private-password"
    )
    api = service.SensorThings(settings, Event())
    try:
        for _ in range(2):
            assert service.run_cycle(api, settings, api.stop)
    finally:
        api.close()
    logins = [kwargs for method, _, kwargs in calls if method == "POST"]
    assert len(logins) == 2
    assert all(item["data"]["password"] == "private-password" for item in logins)
    assert all(
        kwargs["headers"]["Authorization"] == "Bearer private-token"
        for method, _, kwargs in calls
        if method == "GET"
    )
    assert "private-" not in capsys.readouterr().out
    assert "private-" not in repr(settings)


def test_transient_api_failure_retried_by_client(transport):
    routes, calls = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = [
        (503, {}),
        requests.Timeout("unavailable"),
        (200, {"value": []}),
    ]
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    try:
        assert service.run_cycle(api, settings, api.stop)
    finally:
        api.close()
    assert len(calls) == 3


def test_failed_datastream_does_not_block_others_or_leak_error_body(
    transport, capsys, caplog
):
    routes, _ = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (
        200,
        {"value": [configuration(), configuration(2)]},
    )
    routes["GET", BASE + "/Datastreams(1)"] = (
        500,
        {"detail": "private server diagnostics"},
    )
    routes["GET", BASE + "/Datastreams(2)"] = (200, datastream(2))
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    try:
        assert not service.run_cycle(api, settings, api.stop)
    finally:
        api.close()
    assert json.loads(capsys.readouterr().out)["datastreamId"] == 2
    assert "HTTP 500" in caplog.text
    assert "private server diagnostics" not in caplog.text


@pytest.mark.parametrize(
    "body",
    [
        [],
        {},
        {"value": None},
        {"value": [None]},
        {"value": [], "@iot.nextLink": 1},
        {
            "value": [],
            "@iot.nextLink": BASE + "/AggregationConfigurations?enabled=true&$top=100",
        },
        {"value": [], "@iot.nextLink": "https://other.example/admin"},
    ],
)
def test_invalid_collections_fail_the_cycle(transport, body):
    routes, _ = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (
        200,
        body,
    )
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    try:
        assert not service.run_cycle(api, settings, api.stop)
    finally:
        api.close()


@pytest.mark.parametrize(
    "overrides,warning",
    [
        ({"properties": {}}, "samplingFrequency"),
        (
            {"properties": {"samplingFrequency": "PT10M", "constraints": "invalid"}},
            "constraints",
        ),
        (
            {"properties": {"samplingFrequency": "PT10M", "constraints": [{}] * 29}},
            "capacity",
        ),
    ],
)
def test_metadata_problems_are_reported(overrides, warning):
    result = service.describe(
        configuration(), {**datastream(), **overrides}, service.Settings(BASE)
    )
    assert any(warning in item for item in result["warnings"])


def test_event_metadata_needs_no_sampling_frequency():
    result = service.describe(
        configuration(mode="event"), {"properties": {}}, service.Settings(BASE)
    )
    assert not result["warnings"]
    assert result["constraintCount"] == 0


@pytest.mark.parametrize(
    "schedule,expected",
    [
        ("*/10 * * * *", datetime(2026, 11, 6, 12, 10, tzinfo=UTC)),
        ("0 * * * *", datetime(2026, 11, 6, 13, tzinfo=UTC)),
        ("0 0 * * 0", datetime(2026, 11, 8, tzinfo=UTC)),
        ("0 0 * * 7", datetime(2026, 11, 8, tzinfo=UTC)),
        ("0 0 7 * 0", datetime(2026, 11, 7, tzinfo=UTC)),
    ],
)
def test_cron_uses_fixed_utc_times_and_crontab_day_semantics(schedule, expected):
    assert (
        service.next_execution(schedule, datetime(2026, 11, 6, 12, 3, tzinfo=UTC))
        == expected
    )


@pytest.mark.parametrize(
    "key,value",
    [
        ("AGGREGATION_SCHEDULE", "* * * * * *"),
        ("AGGREGATION_SCHEDULE", "not cron"),
        ("AGGREGATION_SCHEDULE", "0 0 31 2 *"),
        ("AGGREGATION_API_URL", "ftp://api/root"),
        ("AGGREGATION_API_URL", "http://user:password@api/root"),
        ("AGGREGATION_UTC_OFFSET_MINUTES", "841"),
        ("AGGREGATION_REQUEST_TIMEOUT_SECONDS", "nan"),
        ("AGGREGATION_REQUEST_TIMEOUT_SECONDS", "0"),
        ("AGGREGATION_RUN_ON_STARTUP", "yes"),
        ("AGGREGATION_WRITE_BATCH_SIZE", "0"),
        ("AGGREGATION_WRITE_BATCH_SIZE", "10001"),
        ("AGGREGATION_WRITE_BATCH_SIZE", "bad"),
    ],
)
def test_invalid_settings_fail_at_startup(monkeypatch, key, value):
    monkeypatch.setenv("AUTHORIZATION", "0")
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        service.Settings.from_env()


def test_authentication_requires_credentials(monkeypatch):
    monkeypatch.setenv("AUTHORIZATION", "1")
    monkeypatch.delenv("ISTSOS_ADMIN", raising=False)
    monkeypatch.delenv("ISTSOS_ADMIN_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="required"):
        service.Settings.from_env()


def test_scheduler_skips_overlap_and_keeps_clock_alignment(caplog):
    current = [datetime(2026, 11, 6, 12, 3, tzinfo=UTC)]
    calls = []

    class Stop:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            current[0] += timedelta(seconds=seconds)

    stop = Stop()

    def task():
        calls.append(current[0])
        if len(calls) == 1:
            current[0] += timedelta(minutes=25)
        else:
            stop.stopped = True

    service.serve(service.Settings(BASE), stop, task, now=lambda: current[0])
    assert calls == [
        datetime(2026, 11, 6, 12, 10, tzinfo=UTC),
        datetime(2026, 11, 6, 12, 40, tzinfo=UTC),
    ]
    assert "Skipped overlapping" in caplog.text


def test_disabled_once_performs_no_api_requests(monkeypatch, transport):
    _, calls = transport
    monkeypatch.setenv("AGGREGATION_ENABLED", "false")
    monkeypatch.setattr("sys.argv", ["aggregation", "--once"])
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    assert service.main() == 0
    assert calls == []


@pytest.mark.parametrize("ignore,expected", [(False, "enabled"), (True, "disabled")])
def test_discovery_prints_quality_filtering_policy(ignore, expected):
    config = configuration(ignoreQuality=ignore)
    result = service.describe(config, datastream(), service.Settings(BASE))
    assert result["qualityFiltering"] == expected
    assert result["configuration"]["ignoreQuality"] is ignore
    assert result["configuration"]["minInputQuality"] == 3


class RecordingState:
    def __init__(self):
        self.events = []

    def start(self, identifier):
        self.events.append(("start", identifier))
        return identifier

    def finish(self, identifier, token, status, error=None):
        self.events.append(("finish", identifier, status, error))

    def discovery_failed(self, message):
        self.events.append(("discovery_failed", message))


def test_service_records_success_failure_and_partial_state(transport, capsys):
    routes, _ = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (
        200,
        {"value": [configuration(), configuration(2), configuration(3)]},
    )
    routes["GET", BASE + "/Datastreams(1)"] = (200, datastream())
    routes["GET", BASE + "/Datastreams(2)"] = (404, {})
    routes["GET", BASE + "/Datastreams(3)"] = (200, {**datastream(3), "properties": {}})
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    finished = [event for event in state.events if event[0] == "finish"]
    assert [event[2] for event in finished] == ["success", "failed", "partial"]
    assert finished[1][3]["step"] == "datastream_read"
    assert finished[2][3]["step"] == "metadata_validation"


def test_collection_failure_is_recorded(transport):
    routes, _ = transport
    routes["GET", BASE + "/AggregationConfigurations?enabled=true&$top=100"] = (500, {})
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    assert state.events == [("discovery_failed", "API returned HTTP 500")]
