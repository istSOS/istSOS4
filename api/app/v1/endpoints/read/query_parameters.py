# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timezone

from app import VERSIONING
from dateutil.parser import isoparse
from fastapi import Depends, HTTPException, Query, status


def _validation_error(message):
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail={
            "code": 422,
            "type": "error",
            "message": message,
        },
    )


def _parse_iso_datetime(value, parameter_name):
    try:
        parsed = isoparse(value)
    except (TypeError, ValueError) as exc:
        raise _validation_error(
            f"Invalid {parameter_name}: expected an ISO 8601 datetime"
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)

    return parsed.astimezone(timezone.utc)


def _validate_time_travel_params(as_of, from_to):
    if as_of and from_to:
        raise _validation_error("$as_of and $from_to cannot be used together")

    if as_of:
        as_of_value = _parse_iso_datetime(as_of, "$as_of")
        if as_of_value > datetime.now(timezone.utc):
            raise _validation_error("$as_of value cannot be in the future")

    if from_to:
        values = from_to.split("/", 1)
        if len(values) != 2 or not values[0] or not values[1]:
            raise _validation_error(
                "Invalid $from_to: expected format is $from_to=<start>/<end>"
            )

        start = _parse_iso_datetime(values[0], "$from_to start")
        end = _parse_iso_datetime(values[1], "$from_to end")
        if start > end:
            raise _validation_error(
                "$from_to start cannot be greater than $from_to end"
            )


class CommonQueryParams:

    def __init__(
        self,
        skip: int = Query(
            None,
            alias="$skip",
            description="The number of elements to skip from the collection",
        ),
        top: int = Query(
            None, alias="$top", description="The number of elements to return"
        ),
        count: bool = Query(
            None,
            alias="$count",
            description="Flag indicating if the total number of items in the collection should be returned.",
        ),
        order: str = Query(
            None,
            alias="$orderby",
            description="The order in which the elements should be returned",
        ),
        select: str = Query(
            None,
            alias="$select",
            description="The list of properties that need to be returned",
        ),
        expand: str = Query(
            None,
            alias="$expand",
            description="The list of related queries that need to be included in the result",
        ),
        filter: str = Query(
            None, alias="$filter", description="A filter query"
        ),
        as_of: str = Query(
            None,
            alias="$as_of",
            description="A date-time parameter to specify the exact moment for which the data is requested (ISO 8601 time string)",
            include_in_schema=VERSIONING,
        ),
        from_to: str = Query(
            None,
            alias="$from_to",
            description="A period parameter to specify the time interval for which the data is requested (ISO 8601 time interval)",
            include_in_schema=VERSIONING,
        ),
    ):
        self.skip = skip
        self.top = top
        self.count = count
        self.order = order
        self.select = select
        self.expand = expand
        self.filter = filter
        self.as_of = as_of
        self.from_to = from_to
        _validate_time_travel_params(as_of, from_to)


def get_common_query_params(
    params: CommonQueryParams = Depends(),
) -> CommonQueryParams:
    return params
