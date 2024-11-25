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
    ):
        self.skip = skip
        self.top = top
        self.count = count
        self.select = select
        self.expand = expand
        self.filter = filter


def get_common_query_params(
    params: CommonQueryParams = Depends(),
) -> CommonQueryParams:
    return params
