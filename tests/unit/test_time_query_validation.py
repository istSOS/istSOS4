from app.v1.endpoints.read.query_parameters import (
    CommonQueryParams,
    validate_time_query_params,
)
import pytest


@pytest.mark.parametrize(
    "as_of, from_to",
    [
        (None, None),
        ("2020-01-01T12:00:00Z", None),
        ("2020-01-01T12:00:00.123Z", None),
        ("2020-01-01T12:00:00", None),
        (
            None,
            "2020-01-01T00:00:00Z/2020-01-02T00:00:00Z",
        ),
        (
            None,
            "2020-01-01T00:00:00Z/2020-01-01T00:00:00Z",
        ),
        (
            None,
            "2020-01-01T10:00:00+02:00/2020-01-01T09:00:00Z",
        ),
        (
            None,
            "2020-01-01T00:00:00/2020-01-03T00:00:00Z",
        ),
    ],
    ids=[
        "no-time-parameters",
        "as-of-utc",
        "as-of-fractional-seconds",
        "as-of-without-timezone",
        "ordered-interval",
        "equal-boundaries",
        "different-timezones",
        "mixed-timezone-presence",
    ],
)
def test_valid_time_parameters_are_accepted(as_of, from_to):
    params = CommonQueryParams(
        as_of=as_of,
        from_to=from_to,
    )

    validate_time_query_params(params)
