"""SQL generation checks for expansion after root-entity pagination."""

import re

import pytest
from app.sta2rest.sta2rest import STA2REST


@pytest.mark.parametrize(
    ("path", "parent_column"),
    [
        (
            "/istsos4/v1.1/Things?$top=2&$skip=3&$expand=Locations",
            "__sta_expand_parent_id",
        ),
        (
            "/istsos4/v1.1/Things?$top=2&$skip=3&$expand=Datastreams",
            "__sta_expand_parent_id",
        ),
        (
            "/istsos4/v1.1/Datastreams?$top=2&$skip=3&$expand=Sensor",
            "__sta_expand_fk_sensor_id",
        ),
        (
            "/istsos4/v1.1/Datastreams?$top=2&$skip=3&$expand=Thing",
            "__sta_expand_fk_thing_id",
        ),
        (
            "/istsos4/v1.1/Observations?$top=2&$skip=3&$expand=FeatureOfInterest",
            "__sta_expand_fk_featuresofinterest_id",
        ),
        (
            "/istsos4/v1.1/HistoricalLocations?$top=2&$skip=3&$expand=Location",
            "__sta_expand_parent_id",
        ),
        (
            "/istsos4/v1.1/Datastreams?$top=2&$skip=3&$expand=Observations",
            "__sta_expand_parent_id",
        ),
        (
            "/istsos4/v1.1/Locations?$top=2&$skip=3&$expand=Things",
            "__sta_expand_parent_id",
        ),
    ],
)
def test_root_expansion_uses_the_paged_parent_row(path, parent_column):
    """The volatile expansion call must correlate to the already paged row."""
    query = STA2REST.convert_query(path)["main_query"]

    assert "LIMIT 3 OFFSET 3" in query
    assert re.search(rf"anon_\d+\.{parent_column}", query)
    assert re.search(r"anon_\d+\.__sta_page_order", query)
