from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.v1.endpoints.read.query_parameters import CommonQueryParams


def _future_datetime():
    return (datetime.now(timezone.utc) + timedelta(days=1)).isoformat().replace(
        "+00:00", "Z"
    )


def test_common_query_params_accepts_valid_as_of():
    params = CommonQueryParams(as_of="2024-01-01T00:00:00Z")

    assert params.as_of == "2024-01-01T00:00:00Z"


def test_common_query_params_rejects_invalid_as_of():
    with pytest.raises(HTTPException) as exc_info:
        CommonQueryParams(as_of="not-a-date")

    assert exc_info.value.status_code == 422
    assert "$as_of" in exc_info.value.detail["message"]


def test_common_query_params_rejects_future_as_of():
    with pytest.raises(HTTPException) as exc_info:
        CommonQueryParams(as_of=_future_datetime())

    assert exc_info.value.status_code == 422
    assert "future" in exc_info.value.detail["message"]


def test_common_query_params_accepts_valid_from_to():
    params = CommonQueryParams(
        from_to="2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"
    )

    assert params.from_to == "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"


@pytest.mark.parametrize(
    "from_to",
    [
        "2024-01-01T00:00:00Z",
        "/2024-01-02T00:00:00Z",
        "2024-01-01T00:00:00Z/",
        "not-a-date/2024-01-02T00:00:00Z",
    ],
)
def test_common_query_params_rejects_invalid_from_to(from_to):
    with pytest.raises(HTTPException) as exc_info:
        CommonQueryParams(from_to=from_to)

    assert exc_info.value.status_code == 422


def test_common_query_params_rejects_reversed_from_to():
    with pytest.raises(HTTPException) as exc_info:
        CommonQueryParams(
            from_to="2024-01-02T00:00:00Z/2024-01-01T00:00:00Z"
        )

    assert exc_info.value.status_code == 422
    assert "greater" in exc_info.value.detail["message"]


def test_common_query_params_rejects_as_of_with_from_to():
    with pytest.raises(HTTPException) as exc_info:
        CommonQueryParams(
            as_of="2024-01-01T00:00:00Z",
            from_to="2024-01-01T00:00:00Z/2024-01-02T00:00:00Z",
        )

    assert exc_info.value.status_code == 422
    assert "cannot be used together" in exc_info.value.detail["message"]
