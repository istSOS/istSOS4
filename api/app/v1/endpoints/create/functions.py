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
from datetime import datetime

from app import AUTHORIZATION, EPSG, HOSTNAME, SUBPATH, ST_AGGREGATE, VERSION, VERSIONING
from app.utils.utils import (
    check_iot_id_in_payload,
    check_missing_properties,
    handle_datetime_fields,
    handle_result_field,
    validate_epsg,
)
from app.v1.endpoints.functions import insert_commit
from app.v1.endpoints.update.datastream import update_datastream_entity
from app.v1.endpoints.update.observation import update_observation_entity
from asyncpg.types import Range


async def set_commit(connection, commit_message, current_user):
    if not (VERSIONING or AUTHORIZATION):
        return None

    if current_user and current_user["role"] == "sensor":
        if commit_message:
            await connection.execute("RESET ROLE;")
            raise Exception("Sensor cannot provide commit message")

        return await connection.fetchval(
            """
                SELECT id FROM sensorthings."Commit"
                WHERE user_id = $1::bigint
            """,
            current_user["id"],
        )

    if not commit_message:
        await connection.execute("RESET ROLE;")
        raise Exception("No commit message provided")

    commit = {
        "message": commit_message,
        "author": current_user["uri"] if current_user else "anonymous",
        "encodingType": "text/plain",
    }

    if current_user:
        commit["user_id"] = current_user["id"]

    return await insert_commit(connection, commit, "CREATE")


async def create_entity(connection, entity_name, payload):
    async with connection.transaction():
        for key in list(payload.keys()):
            if isinstance(payload[key], dict):
                payload[key] = json.dumps(payload[key])

        keys = ", ".join(f'"{key}"' for key in payload.keys())
        values_placeholders = ", ".join(
            (
                f"${i+1}"
                if key != "location" or key != "feature"
                else f"ST_GeomFromGeoJSON(${i+1})"
            )
            for i in range(len(payload))
        )
        insert_query = f"""
            INSERT INTO sensorthings."{entity_name}" ({keys})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        inserted_id = await connection.fetchval(
            insert_query, *payload.values()
        )
        if entity_name == "ObservedProperty":
            entity_name = "ObservedProperties"
        else:
            if entity_name != "FeaturesOfInterest":
                entity_name = f"{entity_name}s"
        inserted_self_link = (
            f"{HOSTNAME}{SUBPATH}{VERSION}/{entity_name}({inserted_id})"
        )

        return inserted_id, inserted_self_link


async def insert_location_entity(connection, payload, commit_id):
    async with connection.transaction():
        thing_id = None
        new_thing = False
        things = []

        if payload["location"]:
            validate_epsg(payload["location"])

        for thing in payload.get("Things", []):
            thing_id = thing.get("@iot.id")
            if thing_id is not None:
                new_thing = False
                check_iot_id_in_payload(thing, "Thing")
            else:
                thing_id, _ = await insert_thing_entity(
                    connection, thing, commit_id
                )
                new_thing = True

            things.append((thing_id, new_thing))

        payload.pop("Things", None)

        if commit_id is not None:
            payload["commit_id"] = commit_id

        location_id, location_self_link = await create_entity(
            connection, "Location", payload
        )

        for thing_id, new_thing in things:
            await manage_thing_location_with_historical_location(
                connection,
                thing_id,
                location_id,
                new_thing,
                commit_id=commit_id,
            )

        return location_id, location_self_link


async def insert_thing_entity(connection, payload, commit_id):
    async with connection.transaction():
        location_id = None
        locations_ids = []

        for location in payload.get("Locations", []):
            location_id = location.get("@iot.id")
            if location_id is None:
                location_id, _ = await insert_location_entity(
                    connection, location, commit_id
                )
            else:
                check_iot_id_in_payload(location, "Location")

            locations_ids.append(location_id)

        payload.pop("Locations", None)

        datastreams = payload.pop("Datastreams", [])

        if commit_id is not None:
            payload["commit_id"] = commit_id

        thing_id, thing_selfLink = await create_entity(
            connection, "Thing", payload
        )

        for location_id in locations_ids:
            await manage_thing_location_with_historical_location(
                connection,
                thing_id,
                location_id,
                True,
                commit_id=commit_id,
            )

        for datastream in datastreams:
            await insert_datastream_entity(
                connection,
                datastream,
                thing_id=thing_id,
                commit_id=commit_id,
            )

        return thing_id, thing_selfLink


async def insert_historical_location_entity(connection, payload, commit_id):
    async with connection.transaction():
        new_thing = False
        thing_id = None
        location_id = None
        location_ids = []

        for location in payload.get("Locations", []):
            location_id = location.get("@iot.id")
            if location_id is None:
                location_id, _ = await insert_location_entity(
                    connection, location, commit_id
                )
            else:
                check_iot_id_in_payload(location, "Location")

            location_ids.append(location_id)

        payload.pop("Locations", None)

        if "Thing" in payload:
            thing_id = payload["Thing"].get("@iot.id")
            if thing_id is None:
                thing_id, _ = await insert_thing_entity(
                    connection, payload["Thing"], commit_id
                )
                new_thing = True
            payload["thing_id"] = thing_id
            payload.pop("Thing", None)

        handle_datetime_fields(payload)

        if commit_id is not None:
            payload["commit_id"] = commit_id

        historical_location_id, historical_location_selfLink = (
            await create_entity(connection, "HistoricalLocation", payload)
        )

        for location_id in location_ids:
            await manage_thing_location_with_historical_location(
                connection,
                thing_id,
                location_id,
                new_thing,
                historical_location_id,
                commit_id=commit_id,
            )

        return historical_location_id, historical_location_selfLink


async def insert_sensor_entity(connection, payload, commit_id):
    async with connection.transaction():

        if commit_id is not None:
            payload["commit_id"] = commit_id

        sensor_id, sensor_selfLink = await create_entity(
            connection, "Sensor", payload
        )

        datastreams = payload.pop("Datastreams", [])
        if datastreams:
            for datastream in datastreams:
                await insert_datastream_entity(
                    connection,
                    datastream,
                    sensor_id=sensor_id,
                    commit_id=commit_id,
                )

        return sensor_id, sensor_selfLink


async def insert_observed_property_entity(connection, payload, commit_id):
    async with connection.transaction():
        if commit_id is not None:
            payload["commit_id"] = commit_id

        observed_property_id, observed_property_selfLink = await create_entity(
            connection, "ObservedProperty", payload
        )

        datastreams = payload.pop("Datastreams", [])
        if datastreams:
            for datastream in datastreams:
                await insert_datastream_entity(
                    connection,
                    datastream,
                    observed_property_id=observed_property_id,
                    commit_id=commit_id,
                )

        return observed_property_id, observed_property_selfLink


async def insert_datastream_entity(
    connection,
    payload,
    thing_id=None,
    sensor_id=None,
    observed_property_id=None,
    commit_id=None,
):
    async with connection.transaction():
        if "@iot.id" in payload:
            check_iot_id_in_payload(payload, "Datastream")

            if thing_id is not None:
                payload["Thing"] = {"@iot.id": thing_id}
            if sensor_id is not None:
                payload["Sensor"] = {"@iot.id": sensor_id}
            if observed_property_id is not None:
                payload["ObservedProperty"] = {"@iot.id": observed_property_id}

            iot_id = payload.pop("@iot.id")
            await update_datastream_entity(connection, iot_id, payload)

            return (
                payload["@iot.id"],
                f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({iot_id})",
            )

        await handle_associations(
            payload,
            "Thing",
            thing_id,
            insert_thing_entity,
            connection,
            commit_id,
        )

        await handle_associations(
            payload,
            "Sensor",
            sensor_id,
            insert_sensor_entity,
            connection,
            commit_id,
        )

        await handle_associations(
            payload,
            "ObservedProperty",
            observed_property_id,
            insert_observed_property_entity,
            connection,
            commit_id,
        )

        check_missing_properties(
            payload, ["Thing", "Sensor", "ObservedProperty"]
        )

        observations = []
        if payload.get("Observations"):
            observations = payload.pop("Observations", [])

        handle_datetime_fields(payload, True)

        if commit_id is not None:
            payload["commit_id"] = commit_id

        datastream_id, datastream_selfLink = await create_entity(
            connection, "Datastream", payload
        )

        if observations:
            for observation in observations:
                await insert_observation_entity(
                    connection,
                    observation,
                    datastream_id=datastream_id,
                    commit_id=commit_id,
                )

    return datastream_id, datastream_selfLink


async def insert_feature_of_interest_entity(
    connection, payload, datastream_id=None, commit_id=None
):
    async with connection.transaction():
        if payload["feature"]:
            validate_epsg(payload["feature"])

        if commit_id is not None:
            payload["commit_id"] = commit_id

        features_of_interest_id, feature_of_interest_self_link = (
            await create_entity(connection, "FeaturesOfInterest", payload)
        )

        if datastream_id is not None:
            await update_datastream_last_foi_id(
                connection, features_of_interest_id, datastream_id
            )

        observations = payload.pop("Observations", [])
        if observations:
            for observation in observations:
                await insert_observation_entity(
                    connection,
                    observation,
                    features_of_interest_id=features_of_interest_id,
                    commit_id=commit_id,
                )

        return features_of_interest_id, feature_of_interest_self_link


async def insert_observation_entity(
    connection,
    payload,
    datastream_id=None,
    features_of_interest_id=None,
    commit_id=None,
):
    async with connection.transaction():
        if "@iot.id" in payload:
            check_iot_id_in_payload(payload, "Observation")

            if datastream_id is not None:
                payload["Datastream"] = {"@iot.id": datastream_id}

            if features_of_interest_id is not None:
                payload["FeaturesOfInterest"] = {
                    "@iot.id": features_of_interest_id
                }

            iot_id = payload.pop("@iot.id")
            await update_observation_entity(connection, iot_id, payload)

            return (
                iot_id,
                f"{HOSTNAME}{SUBPATH}{VERSION}/Observations({iot_id})",
            )

        await handle_associations(
            payload,
            "Datastream",
            datastream_id,
            insert_datastream_entity,
            connection,
            commit_id,
        )

        if "FeatureOfInterest" in payload:
            if "@iot.id" in payload["FeatureOfInterest"]:
                features_of_interest_id = payload["FeatureOfInterest"][
                    "@iot.id"
                ]
                check_iot_id_in_payload(
                    payload["FeatureOfInterest"], "FeatureOfInterest"
                )
                select_query = f"""
                        SELECT last_foi_id
                        FROM sensorthings."Datastream"
                        WHERE id = $1::bigint;
                    """
                last_foi_id = await connection.fetchval(
                    select_query, payload["datastream_id"]
                )
                if last_foi_id != features_of_interest_id:
                    await update_datastream_last_foi_id(
                        connection,
                        features_of_interest_id,
                        payload["datastream_id"],
                    )
            else:
                features_of_interest_id, _ = (
                    await insert_feature_of_interest_entity(
                        connection,
                        payload["FeatureOfInterest"],
                        datastream_id=payload["datastream_id"],
                        commit_id=commit_id,
                    )
                )
            payload.pop("FeatureOfInterest", None)
            payload["featuresofinterest_id"] = features_of_interest_id
        else:
            await generate_feature_of_interest(payload, connection, commit_id)

        check_missing_properties(payload, ["Datastream", "FeaturesOfInterest"])
        handle_datetime_fields(payload)
        handle_result_field(payload)

        if payload.get("phenomenonTime") is None:
            current_time = datetime.now()
            payload["phenomenonTime"] = Range(
                current_time,
                current_time,
                upper_inc=True,
            )

        if commit_id is not None:
            payload["commit_id"] = commit_id

        observation_id, observation_self_link = await create_entity(
            connection, "Observation", payload
        )

        update_query = """
                UPDATE sensorthings."Datastream"
                SET "phenomenonTime" = tstzrange(
                    LEAST($1::timestamptz, lower("phenomenonTime")),
                    GREATEST($2::timestamptz, upper("phenomenonTime")),
                    '[]'
                )
                WHERE id = $3::bigint;
            """
        await connection.execute(
            update_query,
            payload["phenomenonTime"].lower,
            payload["phenomenonTime"].upper,
            payload["datastream_id"],
        )

        return observation_id, observation_self_link


async def update_datastream_last_foi_id(conn, foi_id, datastream_id):
    async with conn.transaction():
        update_query = f"""
            UPDATE sensorthings."Datastream" 
            SET last_foi_id = $1::bigint
            WHERE id = $2::bigint;
        """
        await conn.execute(update_query, foi_id, datastream_id)
        await update_datastream_observedArea(conn, datastream_id, foi_id)


async def generate_feature_of_interest(payload, connection, commit_id=None):
    """
    Generates a FeatureOfInterest based on the given payload and connection.

    Args:
        payload (dict): The payload containing the datastream_id.
        conn (connection): The database connection.

    Returns:
        int: The ID of the generated FeatureOfInterest.

    Raises:
        ValueError: If no locations are found for the Thing.
    """

    async with connection.transaction():
        query_location_from_thing_datastream = f"""
            SELECT
                l.id,
                l.name,
                l.description,
                l."encodingType",
                l.location,
                l.properties,
                l.gen_foi_id
            FROM
                sensorthings."Datastream" d
            JOIN
                sensorthings."Thing" t ON d.thing_id = t.id
            JOIN
                sensorthings."Thing_Location" tl ON tl.thing_id = t.id
            JOIN
                sensorthings."Location" l ON l.ID = tl.location_id
            WHERE
                d.id = {payload["datastream_id"]}
        """

        result = await connection.fetch(query_location_from_thing_datastream)

        if result:
            (
                location_id,
                name,
                description,
                encoding_type,
                location,
                properties,
                gen_foi_id,
            ) = result[0]

            if gen_foi_id is None:
                foi_payload = {
                    "name": name,
                    "description": description,
                    "encodingType": encoding_type,
                    "feature": location,
                    "properties": properties,
                }

                if commit_id is not None:
                    foi_payload["commit_id"] = commit_id

                foi_id, _ = await create_entity(
                    connection, "FeaturesOfInterest", foi_payload
                )

                update_query = f"""
                    UPDATE sensorthings."Location" 
                    SET "gen_foi_id" = $1::bigint
                    WHERE id = $2::bigint;
                """
                await connection.execute(update_query, foi_id, location_id)

                await update_datastream_last_foi_id(
                    connection, foi_id, payload["datastream_id"]
                )

                payload["featuresofinterest_id"] = foi_id
            else:
                select_query = """
                    SELECT last_foi_id
                    FROM sensorthings."Datastream"
                    WHERE id = $1::bigint;
                """
                last_foi_id = await connection.fetchval(
                    select_query, payload["datastream_id"]
                )

                select_query = """
                    SELECT id
                    FROM sensorthings."Observation"
                    WHERE "datastream_id" = $1::bigint
                    LIMIT 1;
                """
                observation_ids = await connection.fetch(
                    select_query, payload["datastream_id"]
                )

                if last_foi_id is None or not observation_ids:
                    await update_datastream_last_foi_id(
                        connection, gen_foi_id, payload["datastream_id"]
                    )

                payload["featuresofinterest_id"] = gen_foi_id
        else:
            raise ValueError(
                "Can not generate foi for Thing with no locations."
            )


async def update_datastream_observedArea(conn, datastream_id, foi_id):
    async with conn.transaction():
        if ST_AGGREGATE == "CONVEX_HULL":
            update_query = """
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_ConvexHull(
                    ST_Collect(
                        "observedArea",
                        (
                            SELECT "feature"
                            FROM sensorthings."FeaturesOfInterest"
                            WHERE id = $1
                        )
                    )
                )
                WHERE id = $2;
            """
        else:
            update_query = f"""
                UPDATE sensorthings."Datastream"
                SET "observedArea" = Set_SRID(ST_Extent(
                    ST_Collect(
                        "observedArea",
                        (
                            SELECT "feature"
                            FROM sensorthings."FeaturesOfInterest"
                            WHERE id = $1
                        )
                    ), {EPSG})
                )
                WHERE id = $2;
            """

        await conn.execute(update_query, foi_id, datastream_id)


async def manage_thing_location_with_historical_location(
    conn,
    thing_id,
    location_id,
    new_record,
    historical_location_id=None,
    commit_id=None,
):
    async with conn.transaction():
        if new_record:
            await conn.execute(
                """
                    INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                    VALUES ($1, $2);
                """,
                thing_id,
                location_id,
            )
        else:
            updated = await conn.fetchval(
                """
                    UPDATE sensorthings."Thing_Location"
                    SET "location_id" = $1
                    WHERE "thing_id" = $2
                    RETURNING "thing_id";
                """,
                location_id,
                thing_id,
            )
            if not updated:
                await conn.execute(
                    """
                    INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                    VALUES ($1, $2);
                """,
                    thing_id,
                    location_id,
                )

        if historical_location_id is None:
            if commit_id is not None:
                insert_query = f"""
                    INSERT INTO sensorthings."HistoricalLocation" ("thing_id", "commit_id")
                    VALUES ($1, $2)
                    RETURNING id;
                """
                historical_location_id = await conn.fetchval(
                    insert_query, thing_id, commit_id
                )
            else:
                insert_query = f"""
                    INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                    VALUES ($1)
                    RETURNING id;
                """
                historical_location_id = await conn.fetchval(
                    insert_query, thing_id
                )

        if historical_location_id is not None:
            await conn.execute(
                """
                    INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                    VALUES ($1, $2);
                """,
                location_id,
                historical_location_id,
            )


async def handle_associations(
    payload, key, entity_id, insert_func, conn, commit_id
):
    if entity_id is not None:
        payload[f"{key.lower()}_id"] = entity_id
    elif key in payload:
        if "@iot.id" in payload[key]:
            check_iot_id_in_payload(payload[key], key)
            payload[f"{key.lower()}_id"] = payload[key]["@iot.id"]
        else:
            async with conn.transaction():
                entity_id, _ = await insert_func(
                    conn, payload[key], commit_id=commit_id
                )
            payload[f"{key.lower()}_id"] = entity_id
        payload.pop(key, None)
