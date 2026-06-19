"""
OGC SensorThings API v1.1 — Sensing Core (c01): error handling tests.

Covers:
  req/resource-path/resource-path-to-entities  §9.2 non-existent entity → 404
  req/request-data/query-status-code           §9.3.1 malformed query → 400
"""
from __future__ import annotations

import pytest

from client import format_id

pytestmark = pytest.mark.c01


# ============================================================================
# 14. Error handling
# ============================================================================

class TestErrorHandling:
    """req/resource-path — invalid resource paths must return appropriate HTTP
    error codes.

    18-088 §9.2 / OData 4.0 §9.5.1:
      - Non-existent entity id → 404
      - Property that does not exist on the entity → 404
      - Unknown collection / entity-set → 404

    FROST parity: Capability1Tests.readNonexistentEntity()
    """

    @pytest.mark.parametrize("path", [
        "Things(999999999)",
        "Locations(999999999)",
        "Datastreams(999999999)",
        "Observations(999999999)",
        "Sensors(999999999)",
        "ObservedProperties(999999999)",
        "FeaturesOfInterest(999999999)",
        "HistoricalLocations(999999999)",
    ])
    def test_nonexistent_entity_id_returns_404(self, client, path):
        """req/resource-path — GET /<entity>(max_int) → 404 for non-existent id."""
        resp = client.get(path)
        assert resp.status_code == 404, (
            f"Non-existent entity must return 404, got {resp.status_code} for {path}"
        )

    def test_unknown_property_returns_404(self, client, seed):
        """req/resource-path — /Things(<id>)/nosuchprop → 404.

        18-088 §9.2 / OData §9.5.1: a request to a property that does not exist
        on the entity MUST return 404, not 5xx.
        """
        resp = client.get(f"Things({format_id(seed.thing_id)})/nosuchprop")
        assert resp.status_code == 404, (
            f"Non-existent property must return 404, got {resp.status_code}"
        )

    def test_unknown_collection_returns_404(self, client):
        """req/resource-path — /NonExistentCollection → 404.

        An unknown entity-set name in the resource path must produce a 404,
        not a 5xx server error.
        """
        resp = client.get("NonExistentCollection")
        assert resp.status_code == 404, (
            f"Unknown collection must return 404, got {resp.status_code}"
        )

    def test_property_of_nonexistent_entity_returns_404(self, client):
        """req/resource-path — /Things(999999999)/name → 404 (entity not found)."""
        resp = client.get("Things(999999999)/name")
        assert resp.status_code == 404, (
            f"Property access on non-existent entity must return 404, "
            f"got {resp.status_code}"
        )

    def test_dollar_value_of_nonexistent_entity_returns_404(self, client):
        """req/resource-path — /Things(999999999)/name/$value → 404."""
        resp = client.get("Things(999999999)/name/$value")
        assert resp.status_code == 404, (
            f"$value on non-existent entity must return 404, got {resp.status_code}"
        )

    def test_navigation_from_nonexistent_entity_returns_empty_or_404(self, client):
        """req/resource-path — /Things(999999999)/Datastreams — navigation from a
        non-existent parent.

        18-088 §9.2 does not explicitly mandate that navigation from a non-existent
        parent returns 404 (only entity-by-id is explicit about 404). OData 4.0 allows
        an empty collection here. This test accepts either 404 OR 200 with an empty
        collection ({"value": []}).
        """
        resp = client.get("Things(999999999)/Datastreams")
        if resp.status_code == 200:
            data = resp.json()
            # If 200, the body must be an empty collection (not real data)
            assert "value" in data
            assert data["value"] == [], (
                "Navigation from non-existent parent: 200 response must be empty collection"
            )
        else:
            assert resp.status_code == 404, (
                f"Navigation from non-existent parent: expected 200 (empty) or 404, "
                f"got {resp.status_code}"
            )


# ============================================================================
# 20. req/request-data/query-status-code — malformed queries return 400
# ============================================================================

class TestQueryStatusCode:
    """req/request-data/query-status-code — a request with an invalid system
    query option MUST result in a 4xx response (18-088 §9.3.1).

    istSOS4 returns HTTP 400 with a structured JSON error body containing 'code',
    'type', and 'message' keys.  This class verifies all declared malformed-query
    cases return 400 (not 500) with a parseable error body.

    The server was verified live to return 400 for all cases below:
      - $filter=name eq (incomplete expression, syntax error)
      - $orderby=nosuchprop asc (unknown property)
      - $top=-5 (negative integer)
      - $skip=-1 (negative integer)
      - $filter=bogus(name) (unknown function)
    """

    def _assert_400_with_error_body(self, resp, case_label: str) -> None:
        """Assert status 400 and structured JSON error body."""
        assert resp.status_code == 400, (
            f"req/request-data/query-status-code: {case_label} must return 400, "
            f"got {resp.status_code}; body: {resp.text[:200]}"
        )
        # Must be parseable JSON (not a raw stacktrace)
        try:
            body = resp.json()
        except Exception:
            raise AssertionError(
                f"req/request-data/query-status-code: {case_label} 400 response "
                f"must have a JSON body; got: {resp.text[:200]}"
            )
        # Body must have an error indicator — at minimum one of code/message/error
        has_error_shape = (
            "code" in body
            or "message" in body
            or "error" in body
        )
        assert has_error_shape, (
            f"req/request-data/query-status-code: {case_label} error body must "
            f"contain 'code', 'message', or 'error'; got keys: {list(body.keys())}"
        )

    def test_bad_filter_syntax_returns_400(self, client, seed):
        """req/request-data/query-status-code — $filter with incomplete expression
        returns 400 with a structured error body.

        Malformed: $filter=name eq  (missing RHS operand — syntax error)
        """
        resp = client.get("Things", params={"$filter": "name eq"})
        self._assert_400_with_error_body(resp, "$filter=name eq (syntax error)")

    def test_orderby_unknown_property_returns_400(self, client, seed):
        """req/request-data/query-status-code — $orderby on a nonexistent property
        returns 400 with a structured error body.

        Malformed: $orderby=nosuchprop asc
        """
        resp = client.get("Things", params={"$orderby": "nosuchprop asc"})
        self._assert_400_with_error_body(
            resp, "$orderby=nosuchprop asc (unknown property)"
        )

    def test_negative_top_returns_400(self, client, seed):
        """req/request-data/query-status-code — negative $top returns 400 with a
        structured error body.

        Malformed: $top=-5
        """
        resp = client.get("Things", params={"$top": "-5"})
        self._assert_400_with_error_body(resp, "$top=-5 (negative value)")

    def test_negative_skip_returns_400(self, client, seed):
        """req/request-data/query-status-code — negative $skip returns 400 with a
        structured error body.

        Malformed: $skip=-1
        """
        resp = client.get("Things", params={"$skip": "-1"})
        self._assert_400_with_error_body(resp, "$skip=-1 (negative value)")

    def test_filter_unknown_function_returns_400(self, client, seed):
        """req/request-data/query-status-code — $filter with an unknown function
        returns 400 with a structured error body.

        Malformed: $filter=bogus(name)
        """
        resp = client.get("Things", params={"$filter": "bogus(name)"})
        self._assert_400_with_error_body(
            resp, "$filter=bogus(name) (unknown function)"
        )

    def test_400_body_is_not_stacktrace(self, client, seed):
        """req/request-data/query-status-code — the 400 error body must be a
        structured JSON error, not an HTML/text stacktrace.

        This test is the semantic complement of the per-case 400 checks above:
        it asserts the body starts with '{' (JSON) and not '<' (HTML traceback)
        or a raw Python traceback.
        """
        resp = client.get("Things", params={"$filter": "name eq"})
        assert resp.status_code == 400
        body = resp.text.strip()
        assert body.startswith("{"), (
            f"req/request-data/query-status-code: 400 body must be JSON "
            f"(starts with '{{'), not a stacktrace; got: {body[:80]!r}"
        )
        assert "<html" not in body.lower(), (
            "400 error body must not be an HTML page (stacktrace)"
        )
        assert "Traceback" not in body, (
            "400 error body must not contain a raw Python traceback"
        )
