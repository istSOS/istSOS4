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
        "unitOfMeasurement": {
            "name": "Celsius",
            "symbol": "C",
            "definition": "test",
        },
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


@pytest.fixture
def calculated(monkeypatch):
    """Replace the engine: record which configurations reach the calculation."""
    calls = []

    def calculate(api, state, configuration, datastream, *args):
        calls.append((configuration, datastream))
        return 0

    monkeypatch.setattr(service, "calculate", calculate)
    return calls


def test_client_reads_all_pages_and_datastreams_with_public_nextlink(
    transport, calculated
):
    routes, calls = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (
        200,
        {
            "value": [configuration()],
            "@iot.nextLink": "https://public.example/istsos4/v1.1/Aggregations?enabled=true&$top=100&$skip=100",
        },
    )
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100&$skip=100"] = (
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
        assert service.run_cycle(api, settings, api.stop, RecordingState())
    finally:
        api.close()
    assert [config["datastreamId"] for config, _ in calculated] == [1, 2]
    assert calculated[1][0]["utcOffsetMinutes"] == 0
    assert calculated[0][1]["properties"]["samplingFrequency"] == "PT10M"
    assert all(method == "GET" for method, _, _ in calls)
    assert all(url.startswith(BASE) for _, url, _ in calls)
    assert all(kwargs["timeout"] == 30 for _, _, kwargs in calls)


def test_project_client_handles_login_and_expiry_and_each_cycle(
    transport, calculated, capsys
):
    routes, calls = transport
    routes["POST", BASE + "/Login"] = (
        200,
        {"access_token": "private-token", "expires_in": 3600},
    )
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (
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
            assert service.run_cycle(api, settings, api.stop, RecordingState())
    finally:
        api.close()
    logins = [kwargs for method, _, kwargs in calls if method == "POST"]
    assert len(logins) == 2
    assert all(
        item["data"]["password"] == "private-password" for item in logins
    )
    assert all(
        kwargs["headers"]["Authorization"] == "Bearer private-token"
        for method, _, kwargs in calls
        if method == "GET"
    )
    assert "private-" not in capsys.readouterr().out
    assert "private-" not in repr(settings)


def test_transient_api_failure_retried_by_client(transport):
    routes, calls = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = [
        (503, {}),
        requests.Timeout("unavailable"),
        (200, {"value": []}),
    ]
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    try:
        assert service.run_cycle(api, settings, api.stop, RecordingState())
    finally:
        api.close()
    assert len(calls) == 3


def test_failed_datastream_does_not_block_others_or_leak_error_body(
    transport, calculated, caplog
):
    routes, _ = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (
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
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    assert [config["datastreamId"] for config, _ in calculated] == [2]
    assert state.events[1][:3] == ("finish", 1, "failed")
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
            "@iot.nextLink": BASE + "/Aggregations?enabled=true&$top=100",
        },
        {"value": [], "@iot.nextLink": "https://other.example/admin"},
    ],
)
def test_invalid_collections_fail_the_cycle(transport, body):
    routes, _ = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (
        200,
        body,
    )
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    assert state.events[0][0] == "discovery_failed"


@pytest.mark.parametrize(
    "overrides,warning",
    [
        ({"properties": {}}, "samplingFrequency"),
        (
            {
                "properties": {
                    "samplingFrequency": "PT10M",
                    "constraints": "invalid",
                }
            },
            "constraints",
        ),
        (
            {
                "properties": {
                    "samplingFrequency": "PT10M",
                    "constraints": [{}] * 29,
                }
            },
            "capacity",
        ),
    ],
)
def test_metadata_problems_are_reported(overrides, warning):
    warnings = service.metadata_warnings(
        configuration(), {**datastream(), **overrides}
    )
    assert any(warning in item for item in warnings)


def test_event_metadata_needs_no_sampling_frequency():
    assert not service.metadata_warnings(
        configuration(mode="event"), {"properties": {}}
    )


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
def test_cron_uses_fixed_utc_times_and_crontab_day_semantics(
    schedule, expected
):
    assert (
        service.next_execution(
            schedule, datetime(2026, 11, 6, 12, 3, tzinfo=UTC)
        )
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
        ("AGGREGATION_RUN_ON_STARTUP", "true"),
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
    monkeypatch.setenv("AGGREGATION_ENABLED", "0")
    monkeypatch.setattr("sys.argv", ["aggregation", "--once"])
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    assert service.main() == 0
    assert calls == []


def test_service_records_failures_and_logs_metadata_warnings(
    transport, calculated, caplog
):
    routes, _ = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (
        200,
        {"value": [configuration(), configuration(2), configuration(3)]},
    )
    routes["GET", BASE + "/Datastreams(1)"] = (200, datastream())
    routes["GET", BASE + "/Datastreams(2)"] = (404, {})
    routes["GET", BASE + "/Datastreams(3)"] = (
        200,
        {**datastream(3), "properties": {}},
    )
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    # Successes are closed by the engine; run_cycle records only failures.
    finished = [event for event in state.events if event[0] == "finish"]
    assert [event[1:3] for event in finished] == [(2, "failed")]
    assert finished[0][3]["step"] == "aggregation"
    assert [config["datastreamId"] for config, _ in calculated] == [1, 3]
    assert (
        "Datastream 3: Sampled Datastream has no samplingFrequency"
        in caplog.text
    )


def test_collection_failure_is_recorded(transport):
    routes, _ = transport
    routes["GET", BASE + "/Aggregations?enabled=true&$top=100"] = (500, {})
    settings = service.Settings(BASE)
    api = service.SensorThings(settings, Event())
    state = RecordingState()
    try:
        assert not service.run_cycle(api, settings, api.stop, state)
    finally:
        api.close()
    assert state.events == [("discovery_failed", "API returned HTTP 500")]


def test_changed_times_asks_current_versions_opened_in_the_window(
    monkeypatch,
):
    api = service.SensorThings(service.Settings("http://api/v1"), Event())
    requested = {}

    def pages(identifier, options):
        requested.update(options, identifier=identifier)
        yield [
            {"phenomenonTime": "2026-01-01T00:10:00Z"},
            {"phenomenonTime": "2026-01-01T00:10:00Z"},
            {"phenomenonTime": "2026-01-01T00:20:00Z/2026-01-01T00:20:00Z"},
        ]

    monkeypatch.setattr(api, "observation_pages", pages)
    since = datetime(2026, 1, 2, tzinfo=UTC)
    until = since + timedelta(minutes=10)
    times = api.changed_times(7, since, until)
    assert times == {"2026-01-01T00:10:00Z", "2026-01-01T00:20:00Z"}
    assert requested["identifier"] == 7
    assert "$from_to" not in requested and "$as_of" not in requested
    assert requested["$filter"] == (
        f"lower(systemTimeValidity) ge {since.isoformat()} and "
        f"lower(systemTimeValidity) le {until.isoformat()}"
    )
