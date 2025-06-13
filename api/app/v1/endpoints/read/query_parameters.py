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
from fastapi import Depends, Query


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


def get_common_query_params(
    params: CommonQueryParams = Depends(),
) -> CommonQueryParams:
    return params
