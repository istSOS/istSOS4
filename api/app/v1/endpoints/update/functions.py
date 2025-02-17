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

import json

from app import AUTHORIZATION, VERSIONING
from app.utils.utils import (
    handle_associations,
    handle_datetime_fields,
    handle_result_field,
    validate_epsg,
)
from app.v1.endpoints.functions import insert_commit


async def check_id_exists(connection, entity_name, entity_id):
    query = f"""
        SELECT id
        FROM sensorthings."{entity_name}"
        WHERE id = $1
    """
    return await connection.fetchval(query, entity_id)


async def set_commit(connection, commit_message, current_user):
    if VERSIONING or AUTHORIZATION:
        if not commit_message:
            raise Exception("No commit message provided")

        commit = {
            "message": commit_message,
            "author": current_user["uri"] if current_user else "anonymous",
            "encodingType": "text/plain",
        }

        if current_user:
            commit["user_id"] = current_user["id"]
            if current_user["role"] != "istsos_sensor":
                return await insert_commit(connection, commit, "UPDATE")
    return None


async def update_entity(
    connection, entity_name, entity_id, payload, obs=False
):
    payload = {
        key: json.dumps(value) if isinstance(value, dict) else value
        for key, value in payload.items()
    }
    set_clause = ", ".join(
        [
            (
                f'"{key}" = ${i + 1}'
                if key != "location" and key != "feature"
                else f'"{key}" = ST_GeomFromGeoJSON(${i + 1})'
            )
            for i, key in enumerate(payload.keys())
        ]
    )
    if obs:
        return await connection.fetchrow(
            f"""
            UPDATE sensorthings."{entity_name}"
            SET {set_clause}
            WHERE id = {entity_id}
            RETURNING "phenomenonTime", "datastream_id";
        """,
            *payload.values(),
        )
    await connection.fetchval(
        f"""
            UPDATE sensorthings."{entity_name}"
            SET {set_clause}
            WHERE id = {entity_id};
        """,
        *payload.values(),
    )


async def update_location_entity(
    connection,
    location_id,
    payload,
):
    if payload["location"]:
        validate_epsg(payload["location"])

    if "Things" in payload:
        if isinstance(payload["Things"], dict):
            payload["Things"] = [payload["Things"]]
        for thing in payload["Things"]:
            if not isinstance(thing, dict) or list(thing.keys()) != [
                "@iot.id"
            ]:
                raise Exception(
                    "Invalid format: Each thing should be a dictionary with a single key '@iot.id'."
                )
            thing_id = thing["@iot.id"]
            check = await connection.fetchval(
                """
                    UPDATE sensorthings."Thing_Location"
                    SET thing_id = $1
                    WHERE location_id = $2;
                """,
                location_id,
                thing_id,
            )
            if check is None:
                await connection.execute(
                    """
                        INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("thing_id", "location_id") DO NOTHING;
                    """,
                    thing_id,
                    location_id,
                )
            historical_location_id = await connection.fetchval(
                """
                    INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                    VALUES ($1)
                    RETURNING id;
                """,
                thing_id,
            )
            await connection.execute(
                """
                    INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                    VALUES ($1, $2)
                    ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING;
                """,
                location_id,
                historical_location_id,
            )
        payload.pop("Things")

    payload["gen_foi_id"] = None

    if payload:
        await update_entity(connection, "Location", location_id, payload)


async def update_thing_entity(connection, thing_id, payload):
    if "Locations" in payload:
        if isinstance(payload["Locations"], dict):
            payload["Locations"] = [payload["Locations"]]
        for location in payload["Locations"]:
            if not isinstance(location, dict) or list(location.keys()) != [
                "@iot.id"
            ]:
                raise Exception(
                    "Invalid format: Each location should be a dictionary with a single key '@iot.id'."
                )
            location_id = location["@iot.id"]
            check = await connection.fetchval(
                """
                    UPDATE sensorthings."Thing_Location"
                    SET location_id = $1
                    WHERE thing_id = $2;
                """,
                location_id,
                thing_id,
            )
            if check is None:
                await connection.execute(
                    """
                        INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("thing_id", "location_id") DO NOTHING;
                    """,
                    thing_id,
                    location_id,
                )
            historical_location_id = await connection.fetchval(
                """
                    INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                    VALUES ($1)
                    RETURNING id;
                """,
                thing_id,
            )
            await connection.execute(
                """
                    INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                    VALUES ($1, $2)
                    ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING;
                """,
                location_id,
                historical_location_id,
            )
        payload.pop("Locations")

    await handle_nested_entities(
        connection,
        payload,
        thing_id,
        "Datastreams",
        "thing_id",
        "Datastream",
    )

    if payload:
        await update_entity(connection, "Thing", thing_id, payload)


async def update_historical_location_entity(
    connection, historical_location_id, payload
):
    if "Locations" in payload:
        if isinstance(payload["Locations"], dict):
            payload["Locations"] = [payload["Locations"]]
        for location in payload["Locations"]:
            if not isinstance(location, dict) or list(location.keys()) != [
                "@iot.id"
            ]:
                raise Exception(
                    "Invalid format: Each location should be a dictionary with a single key '@iot.id'."
                )
            location_id = location["@iot.id"]
            check = await connection.fetchval(
                """
                    UPDATE sensorthings."Location_HistoricalLocation"
                    SET location_id = $1
                    WHERE historicallocation_id = $2;
                """,
                location_id,
                historical_location_id,
            )
            if check is None:
                await connection.execute(
                    """
                        INSERT INTO sensorthings."Location_HistoricalLocation" ("historicallocation_id", "location_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("historicallocation_id", "location_id") DO NOTHING;
                    """,
                    historical_location_id,
                    location_id,
                )
        payload.pop("Locations")

    handle_datetime_fields(payload)

    handle_associations(payload, ["Thing"])

    if payload:
        await update_entity(
            connection,
            "HistoricalLocation",
            historical_location_id,
            payload,
        )


async def update_sensor_entity(connection, sensor_id, payload):
    await handle_nested_entities(
        connection,
        payload,
        sensor_id,
        "Datastreams",
        "sensor_id",
        "Datastream",
    )

    if payload:
        await update_entity(connection, "Sensor", sensor_id, payload)


async def update_observed_property_entity(
    connection, observed_property_id, payload
):
    await handle_nested_entities(
        connection,
        payload,
        observed_property_id,
        "Datastreams",
        "observedproperty_id",
        "Datastream",
    )

    if payload:
        await update_entity(
            connection, "ObservedProperty", observed_property_id, payload
        )


async def update_datastream_entity(connection, datastream_id, payload):
    handle_datetime_fields(payload)

    handle_associations(payload, ["Thing", "Sensor", "ObservedProperty"])

    await handle_nested_entities(
        connection,
        payload,
        datastream_id,
        "Observations",
        "datastream_id",
        "Observation",
    )

    if payload:
        await update_entity(connection, "Datastream", datastream_id, payload)


async def update_feature_of_interest_entity(
    connection, feature_of_interest_id, payload
):
    if payload["feature"]:
        validate_epsg(payload["feature"])

    await handle_nested_entities(
        connection,
        payload,
        feature_of_interest_id,
        "Observations",
        "featuresofinterest_id",
        "Observation",
    )

    if payload:
        await update_entity(
            connection, "FeaturesOfInterest", feature_of_interest_id, payload
        )


async def update_observation_entity(connection, observation_id, payload):
    handle_datetime_fields(payload)

    handle_result_field(payload)

    handle_associations(payload, ["Datastream", "FeatureOfInterest"])

    if payload:
        await update_entity(connection, "Observation", observation_id, payload)


async def handle_nested_entities(
    connection, payload, entity_id, key, field, update_table
):
    async with connection.transaction():
        if key in payload:
            if isinstance(payload[key], dict):
                payload[key] = [payload[key]]
            for item in payload[key]:
                if not isinstance(item, dict) or list(item.keys()) != [
                    "@iot.id"
                ]:
                    raise Exception(
                        f"Invalid format: Each item in '{key}' should be a dictionary with a single key '@iot.id'."
                    )
                related_id = item["@iot.id"]
                await connection.execute(
                    f'UPDATE sensorthings."{update_table}" SET {field} = {entity_id} WHERE id = {related_id};'
                )
            payload.pop(key)
