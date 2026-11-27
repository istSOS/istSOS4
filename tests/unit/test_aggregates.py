from datetime import UTC, datetime

import pytest
from app.v1.endpoints.exceptions import BadRequest
from app.v1.endpoints.read import aggregate


def parse(**params):
    return aggregate.parse_request(
        {"$func": "sum", "$interval": "PT1H", **params}
    )


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"$func": "sum"},
        {"$interval": "PT1H"},
        {"$func": "median", "$interval": "PT1H"},
        {"$func": "sum", "$interval": "PT2H"},
    ],
)
def test_func_and_interval_are_required_and_closed_lists(params):
    with pytest.raises(BadRequest):
        aggregate.parse_request(params)


@pytest.mark.parametrize(
    "name", ["$expand", "$as_of", "$from_to", "$resultFormat"]
)
def test_unsupported_parameters_are_rejected(name):
    with pytest.raises(BadRequest):
        parse(**{name: "x"})


def test_interval_selects_table_and_func_selects_result():
    query = parse(**{"$interval": "P1D", "$func": "count"})
    assert query["table"] == "observations_1day"
    assert query["result"] == "count_numeric"


def test_missing_values_become_minus_999():
    assert parse(**{"$func": "avg"})["result"] == "COALESCE(avg_value, -999.9)"


def test_phenomenon_time_filter_compares_bucket_end():
    query = parse(
        **{
            "$filter": "phenomenonTime ge 2026-11-16T00:00:00Z and result gt 0.5"
        }
    )
    assert query["where"].startswith(
        "(bucket_end >= $2 AND COALESCE(sum_value,"
    )
    assert query["where"].endswith("> $3)")
    assert query["filter_params"] == [datetime(2026, 11, 16, tzinfo=UTC), 0.5]


def test_reversed_comparison_and_not_and_in():
    query = parse(
        **{"$filter": "not (2 lt resultQuality) or resultQuality in (1, 3)"}
    )
    assert (
        query["where"]
        == "((NOT result_quality > $2) OR result_quality IN ($3, $4))"
    )
    assert query["filter_params"] == [2, 1, 3]


@pytest.mark.parametrize(
    "expression",
    [
        "phenomenonTime ge 2026-11-16T00:00:00",  # no timezone
        "phenomenonTime ge 5",
        "result gt '2026-11-16T00:00:00Z'",
        "id eq 1",
        "Datastream/id eq 1",
        "year(phenomenonTime) eq 2026",
        "result gt",
    ],
)
def test_invalid_filters_are_rejected(expression):
    with pytest.raises(BadRequest):
        parse(**{"$filter": expression})


def test_orderby_select_top_skip_count():
    query = parse(
        **{
            "$orderby": "result desc,phenomenonTime",
            "$select": "phenomenonTime,result",
            "$top": "10",
            "$skip": "20",
            "$count": "true",
        }
    )
    assert query["order"][0].endswith(" DESC") and query["order"][1:] == [
        "bucket_end ASC",
        "bucket_end ASC",
    ]
    assert query["select"] == ("phenomenonTime", "result")
    assert (query["top"], query["skip"], query["count"]) == (10, 20, True)


@pytest.mark.parametrize(
    "params",
    [
        {"$orderby": "id desc"},
        {"$select": "@iot.id"},
        {"$top": "-1"},
        {"$count": "yes"},
    ],
)
def test_invalid_paging_and_projection_are_rejected(params):
    with pytest.raises(BadRequest):
        parse(**params)


def test_serialize_shapes_a_bucket_like_an_observation():
    row = {
        "bucket_start": datetime(2026, 11, 16, 0, tzinfo=UTC),
        "bucket_end": datetime(2026, 11, 16, 1, tzinfo=UTC),
        "calculated_at": datetime(2026, 11, 16, 1, 3, tzinfo=UTC),
        "result": 1.2,
        "result_quality": 51,
        "count_total": 6,
        "count_numeric": 6,
        "expected_count": 6,
        "coverage": 1.0,
        "evaluated_until": datetime(2026, 11, 16, 0, 40, tzinfo=UTC),
    }
    item = aggregate.serialize(row, 10, aggregate.OUTPUT)
    assert (
        item["phenomenonTime"] == "2026-11-16T00:00:00Z/2026-11-16T01:00:00Z"
    )
    assert item["resultTime"] == "2026-11-16T01:03:00Z"
    assert "@iot.id" not in item
    assert item["parameters"]["open"] is True
    assert item["Datastream@iot.navigationLink"].endswith("/Datastreams(10)")


def test_route_is_registered_before_the_catch_all():
    from app.v1 import api

    paths = list(api.v1.openapi()["paths"])
    assert any(
        path.endswith("/Datastreams({datastream_id})/Aggregates")
        for path in paths
    )


def test_floating_point_noise_is_removed_from_values():
    assert aggregate.clean(8.799999999999997) == 8.8
    assert aggregate.clean(0.1 + 0.2) == 0.3
    assert aggregate.clean(-999.9) == -999.9
    assert aggregate.clean(12.3456789) == 12.3456789
    assert aggregate.clean(44) == 44 and aggregate.clean(None) is None


def test_query_parameter_dependency_resolves_and_missing_func_is_400():
    # Regression: a dependency parameter named "func" broke FastAPI's threadpool call (500).
    from app.db.asyncpg_db import get_pool
    from app.v1 import api
    from fastapi.testclient import TestClient

    api.v1.dependency_overrides[get_pool] = lambda: None
    try:
        response = TestClient(api.v1).get(
            "/Datastreams(1)/Aggregates?$interval=PT1H&$top=5"
        )
    finally:
        api.v1.dependency_overrides.pop(get_pool, None)
    assert response.status_code == 400
    assert "$func is required" in response.json()["message"]
