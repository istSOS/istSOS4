"""
Tests for selected functions in api/app/v1/endpoints/create/functions.py

Covers set_commit(), handle_associations(), and create_entity().
No live database needed for any of these tests.

Author: Vishmayraj
"""

import sys
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
API_DIR = os.path.join(PROJECT_ROOT, "api")

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, API_DIR)

from app.v1.endpoints.create.functions import (
    set_commit,
    handle_associations,
    create_entity,
)

MODULE = "app.v1.endpoints.create.functions"


def make_connection(fetchval_return=None):
    """Async mock with awaitable execute and fetchval, matching the asyncpg connection contract."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=fetchval_return)
    return conn


# set_commit tests cover all branching logic before the DB call.

class TestSetCommitDisabled:
    """Both VERSIONING and AUTHORIZATION are False, return None immediately, no DB touched."""

    @pytest.mark.asyncio
    async def test_returns_none_when_both_flags_off(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            result = await set_commit(conn, "some message", None)
        assert result is None

    @pytest.mark.asyncio
    async def test_does_not_call_execute_when_disabled(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            await set_commit(conn, "msg", {"role": "admin", "id": 1})
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_call_fetchval_when_disabled(self):
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", False), \
             patch(f"{MODULE}.AUTHORIZATION", False):
            await set_commit(conn, "msg", {"role": "admin", "id": 1})
        conn.fetchval.assert_not_called()


class TestSetCommitSensorRole:
    """Sensor role: forbidden to provide a commit message; without one, fetches their existing commit id."""

    @pytest.mark.asyncio
    async def test_sensor_with_message_raises(self):
        conn = make_connection()
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="Sensor cannot provide commit message"):
                await set_commit(conn, "should not be here", sensor)

    @pytest.mark.asyncio
    async def test_sensor_with_message_resets_role_before_raising(self):
        """RESET ROLE must be called before the exception propagates."""
        conn = make_connection()
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception):
                await set_commit(conn, "bad message", sensor)
        conn.execute.assert_called_once_with("RESET ROLE;")

    @pytest.mark.asyncio
    async def test_sensor_without_message_fetches_existing_commit(self):
        conn = make_connection(fetchval_return=99)
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            result = await set_commit(conn, None, sensor)
        assert result == 99

    @pytest.mark.asyncio
    async def test_sensor_without_message_calls_fetchval_once(self):
        conn = make_connection(fetchval_return=99)
        sensor = {"role": "sensor", "id": 7}
        with patch(f"{MODULE}.VERSIONING", True):
            await set_commit(conn, None, sensor)
        conn.fetchval.assert_called_once()


class TestSetCommitNoMessage:
    """Non-sensor user with no commit message, RESET ROLE then raise, same as the sensor message violation."""

    @pytest.mark.asyncio
    async def test_no_message_raises(self):
        conn = make_connection()
        admin = {"role": "administrator", "id": 1, "uri": "http://example.com/users/1"}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="No commit message provided"):
                await set_commit(conn, None, admin)

    @pytest.mark.asyncio
    async def test_no_message_resets_role_before_raising(self):
        conn = make_connection()
        admin = {"role": "administrator", "id": 1, "uri": "http://example.com/users/1"}
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception):
                await set_commit(conn, None, admin)
        conn.execute.assert_called_once_with("RESET ROLE;")

    @pytest.mark.asyncio
    async def test_anonymous_user_no_message_raises(self):
        """current_user=None with no message must also raise, not crash."""
        conn = make_connection()
        with patch(f"{MODULE}.VERSIONING", True):
            with pytest.raises(Exception, match="No commit message provided"):
                await set_commit(conn, None, None)


class TestSetCommitPayloadBuilding:
    """All preconditions pass -- checks author, user_id, action, and encodingType in the built commit dict."""

    @pytest.mark.asyncio
    async def test_anonymous_author_is_anonymous_string(self):
        conn = make_connection()
        captured = {}

        async def fake_insert_commit(_, commit, __):
            captured.update(commit)
            return 55

        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            result = await set_commit(conn, "init", None)

        assert captured["author"] == "anonymous"
        assert result == 55

    @pytest.mark.asyncio
    async def test_anonymous_user_has_no_user_id_in_commit(self):
        conn = make_connection()
        captured = {}

        async def fake_insert_commit(_, commit, __):
            captured.update(commit)
            return 55

        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            await set_commit(conn, "init", None)

        assert "user_id" not in captured

    @pytest.mark.asyncio
    async def test_authenticated_author_is_user_uri(self):
        conn = make_connection()
        captured = {}

        async def fake_insert_commit(_, commit, __):
            captured.update(commit)
            return 77

        user = {"role": "administrator", "id": 3, "uri": "http://example.com/users/3"}
        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            await set_commit(conn, "update sensor", user)

        assert captured["author"] == "http://example.com/users/3"

    @pytest.mark.asyncio
    async def test_authenticated_user_id_is_set_in_commit(self):
        conn = make_connection()
        captured = {}

        async def fake_insert_commit(_, commit, __):
            captured.update(commit)
            return 77

        user = {"role": "administrator", "id": 3, "uri": "http://example.com/users/3"}
        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            await set_commit(conn, "update sensor", user)

        assert captured["user_id"] == 3

    @pytest.mark.asyncio
    async def test_action_passed_to_insert_commit_is_create(self):
        conn = make_connection()
        captured_action = {}

        async def fake_insert_commit(_, commit, action):
            captured_action["action"] = action
            return 1

        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            await set_commit(conn, "msg", None)

        assert captured_action["action"] == "CREATE"

    @pytest.mark.asyncio
    async def test_encoding_type_is_text_plain(self):
        conn = make_connection()
        captured = {}

        async def fake_insert_commit(_, commit, __):
            captured.update(commit)
            return 1

        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            await set_commit(conn, "msg", None)

        assert captured["encodingType"] == "text/plain"

    @pytest.mark.asyncio
    async def test_returns_id_from_insert_commit(self):
        conn = make_connection()

        async def fake_insert_commit(_, __, ___):
            return 42

        with patch(f"{MODULE}.VERSIONING", True), \
             patch(f"{MODULE}.insert_commit", fake_insert_commit):
            result = await set_commit(conn, "msg", None)

        assert result == 42


# handle_associations has three branches. Only the two pure ones are tested here:
# entity_id passed directly, and @iot.id found in payload. The third branch
# calls insert_func and hits the DB, so it is skipped in this file.

class TestHandleAssociationsEntityIdGiven:
    """entity_id is passed directly, payload gets the id key set, insert_func is never called."""

    @pytest.mark.asyncio
    async def test_sets_lowercase_id_key(self):
        payload = {}
        conn = AsyncMock()
        await handle_associations(payload, "Datastream", 42, AsyncMock(), conn, None)
        assert payload["datastream_id"] == 42

    @pytest.mark.asyncio
    async def test_does_not_call_insert_func(self):
        payload = {}
        conn = AsyncMock()
        insert_func = AsyncMock()
        await handle_associations(payload, "Sensor", 10, insert_func, conn, None)
        insert_func.assert_not_called()

    @pytest.mark.asyncio
    async def test_works_for_observed_property_key(self):
        payload = {}
        conn = AsyncMock()
        await handle_associations(payload, "ObservedProperty", 5, AsyncMock(), conn, None)
        assert payload["observedproperty_id"] == 5


class TestHandleAssociationsIotIdInPayload:
    """entity_id is None but payload has @iot.id, id is extracted, original key removed, no DB call."""

    @pytest.mark.asyncio
    async def test_extracts_iot_id_into_payload(self):
        payload = {"Datastream": {"@iot.id": 99}}
        conn = AsyncMock()
        await handle_associations(payload, "Datastream", None, AsyncMock(), conn, None)
        assert payload["datastream_id"] == 99

    @pytest.mark.asyncio
    async def test_removes_original_key_from_payload(self):
        payload = {"Sensor": {"@iot.id": 3}}
        conn = AsyncMock()
        await handle_associations(payload, "Sensor", None, AsyncMock(), conn, None)
        assert "Sensor" not in payload

    @pytest.mark.asyncio
    async def test_does_not_call_insert_func_for_iot_id(self):
        payload = {"Datastream": {"@iot.id": 7}}
        conn = AsyncMock()
        insert_func = AsyncMock()
        await handle_associations(payload, "Datastream", None, insert_func, conn, None)
        insert_func.assert_not_called()


# create_entity tests check that the correct SQL placeholder is generated
# for geometry columns. ST_GeomFromGeoJSON() should wrap the placeholder
# for "location" and "feature" keys, and plain $N for everything else.

class TestCreateEntityGeometryBranchBug:
    """Geometry columns get ST_GeomFromGeoJSON as the placeholder, non-geometry columns get plain $N."""

    def make_conn_with_query_capture(self):
        """Connection mock that captures the query string passed to fetchval."""
        captured = {}
        conn = AsyncMock()

        async def fake_fetchval(query, *_args):
            captured["query"] = query
            return 1

        conn.fetchval = fake_fetchval
        conn.transaction = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=None),
            __aexit__=AsyncMock(return_value=False),
        ))
        return conn, captured

    @pytest.mark.asyncio
    async def test_location_key_uses_st_geomfromgeojson(self):
        """A location payload should produce ST_GeomFromGeoJSON($1) in the INSERT query, not plain $1."""
        conn, captured = self.make_conn_with_query_capture()
        payload = {"location": '{"type": "Point", "coordinates": [0, 0]}'}

        await create_entity(conn, "Location", payload)

        assert "ST_GeomFromGeoJSON" in captured.get("query", "")

    @pytest.mark.asyncio
    async def test_feature_key_uses_st_geomfromgeojson(self):
        """A feature payload should produce ST_GeomFromGeoJSON($1) in the INSERT query, not plain $1."""
        conn, captured = self.make_conn_with_query_capture()
        payload = {"feature": '{"type": "Point", "coordinates": [1, 1]}'}

        await create_entity(conn, "FeatureOfInterest", payload)

        assert "ST_GeomFromGeoJSON" in captured.get("query", "")

    @pytest.mark.asyncio
    async def test_non_geometry_key_uses_plain_placeholder(self):
        """A non-geometry key like name should always produce a plain $1 placeholder."""
        conn, captured = self.make_conn_with_query_capture()
        payload = {"name": "test sensor"}

        await create_entity(conn, "Sensor", payload)

        query = captured.get("query", "")
        assert "$1" in query
        assert "ST_GeomFromGeoJSON" not in query