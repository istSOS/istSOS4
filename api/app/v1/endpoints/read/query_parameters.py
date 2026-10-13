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

from app import VERSIONING
from fastapi import Depends, Query, HTTPException
from dateutil.parser import isoparse
from datetime import datetime, timezone
import re

from app.sta2rest.sta_parser.lexer import TOKEN_TYPES


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
        filter: str = Query(None, alias="$filter", description="A filter query"),
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


class ObservationQueryParams(CommonQueryParams):

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
        filter: str = Query(None, alias="$filter", description="A filter query"),
        result_format: str = Query(
            None,
            alias="$resultFormat",
            description="Return observations using the Data Array result format",
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
        super().__init__(
            skip,
            top,
            count,
            order,
            select,
            expand,
            filter,
            as_of,
            from_to,
        )
        self.result_format = result_format


def validate_time_query_params(params: CommonQueryParams) -> None:

    now = datetime.now(timezone.utc)
    local_timezone = datetime.now().astimezone().tzinfo

    if params.as_of is not None and params.from_to is not None:
        raise HTTPException(
            status_code=422,
            detail="Query parameters $as_of and $from_to cannot be used together.",
        )

    elif params.as_of is not None:

        if re.fullmatch(TOKEN_TYPES["DATETIME"], params.as_of) is None:
            raise HTTPException(
                status_code=422,
                detail="Query parameter $as_of must be a valid date-time.",
            )

        try:
            as_of_datetime = isoparse(params.as_of)

        except ValueError:
            raise HTTPException(
                status_code=422, detail="Query parameter $as_of must be a date!"
            )

        if as_of_datetime.tzinfo is None:
            as_of_datetime = as_of_datetime.replace(tzinfo=local_timezone)
            as_of_datetime = as_of_datetime.astimezone(timezone.utc)

        if as_of_datetime > now:
            raise HTTPException(
                status_code=422,
                detail="$as_of query parameter time can not be more than current time!",
            )

    elif params.from_to is not None:

        parts = params.from_to.split("/")

        if len(parts) != 2:
            raise HTTPException(
                status_code=422,
                detail="Query parameter $from_to must contain two date-times separated by '/'.",
            )

        for part in parts:
            if not part.strip():
                raise HTTPException(
                    status_code=422,
                    detail="Query parameter $from_to cannot contain empty boundaries.",
                )

            if re.fullmatch(TOKEN_TYPES["DATETIME"], part) is None:
                raise HTTPException(
                    status_code=422,
                    detail="Query parameter $from_to must contain valid date-times.",
                )

        try:
            start_datetime = isoparse(parts[0])
            end_datetime = isoparse(parts[1])

        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="Query parameter $from_to must contain valid date-times.",
            )

        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=local_timezone)
            start_datetime = start_datetime.astimezone(timezone.utc)

        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=local_timezone)
            end_datetime = end_datetime.astimezone(timezone.utc)

        if start_datetime > end_datetime:
            raise HTTPException(
                status_code=422,
                detail="First time interval in quary parameter $from_to can not be more than second time interval!",
            )


def get_common_query_params(
    params: CommonQueryParams = Depends(),
) -> CommonQueryParams:
    validate_time_query_params(params)
    return params


def get_observation_query_params(
    params: ObservationQueryParams = Depends(),
) -> ObservationQueryParams:
    validate_time_query_params(params)
    return params
