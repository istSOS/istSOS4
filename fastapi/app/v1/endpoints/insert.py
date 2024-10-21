import json
import traceback
from datetime import datetime

from app import DEBUG, EPSG, HOSTNAME, SUBPATH, VERSION
from app.db.asyncpg_db import get_pool
from app.sta2rest import sta2rest
from app.utils.utils import handle_datetime_fields, handle_result_field
from app.v1.endpoints.update_patch import updateDatastream, updateObservation
from fastapi.responses import JSONResponse, Response

from fastapi import APIRouter, Depends, Request, status

v1 = APIRouter()

try:
    DEBUG = DEBUG
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0


@v1.api_route("/CreateObservations", methods=["POST"])
async def create_observations(request: Request, pgpool=Depends(get_pool)):
    try:
        body = await request.json()
        if not isinstance(body, list):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "code": 400,
                    "type": "error",
                    "message": "Invalid payload format. Expected a list of observations.",
                },
            )

        response_urls = []

        async with pgpool.acquire() as conn:
            async with conn.transaction():
                for observation_set in body:
                    datastream_id = observation_set.get("Datastream", {}).get(
                        "@iot.id"
                    )
                    components = observation_set.get("components", [])
                    data_array = observation_set.get("dataArray", [])

                    if not datastream_id:
                        return JSONResponse(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": "Missing 'datastream_id' in Datastream.",
                            },
                        )

                    # Check that at least phenomenonTime and result are present
                    if (
                        "phenomenonTime" not in components
                        or "result" not in components
                    ):
                        return JSONResponse(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content={
                                "code": 400,
                                "type": "error",
                                "message": "Missing required properties 'phenomenonTime' or 'result' in components.",
                            },
                        )

                    for data in data_array:
                        try:
                            observation_payload = {
                                components[i]: (
                                    data[i] if i < len(data) else None
                                )
                                for i in range(len(components))
                            }

                            observation_payload["datastream_id"] = (
                                datastream_id
                            )

                            if "FeatureOfInterest/id" in observation_payload:
                                observation_payload["FeatureOfInterest"] = {
                                    "@iot.id": observation_payload.pop(
                                        "FeatureOfInterest/id"
                                    )
                                }
                            else:
                                await generate_feature_of_interest(
                                    observation_payload, conn
                                )

                            _, observation_selfLink = await insertObservation(
                                observation_payload, conn
                            )
                            response_urls.append(observation_selfLink)
                        except Exception as e:
                            response_urls.append("error")
                            if DEBUG:
                                print(f"Error inserting observation: {str(e)}")
                                traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_201_CREATED, content=response_urls
        )

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


@v1.api_route("/{path_name:path}", methods=["POST"])
async def catch_all_post(
    request: Request, path_name: str, pgpool=Depends(get_pool)
):
    """
    Handle POST requests for all paths.

    Args:
        request (Request): The incoming request object.
        path_name (str): The path name extracted from the URL.
        pgpool: The database connection pool.

    Returns:
        JSONResponse: The response containing the result of the request.

    Raises:
        JSONResponse: If the content-type is not application/json or if an exception occurs.
    """
    # Accept only content-type application/json
    if (
        not "content-type" in request.headers
        or request.headers["content-type"] != "application/json"
    ):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": "Only content-type application/json is supported.",
            },
        )

    try:
        full_path = request.url.path
        # parse uri
        result = sta2rest.STA2REST.parse_uri(full_path)
        # get json body
        body = await request.json()

        main_table = result["entity"][0]

        if DEBUG:
            try:
                print(f"POST body {main_table}", body)
                import copy

                b = copy.deepcopy(body)
            except:
                b = ""

        if len(result["entities"]) == 1:
            [name, id] = result["entities"][0]
            relationships = {
                "Observation": ["Datastream", "FeaturesOfInterest"],
                "Location": ["Thing"],
                "Thing": ["Location"],
                "Datastream": ["Thing", "Sensor", "ObservedProperty"],
                "HistoricalLocation": ["Thing"],
            }
            related_entities = relationships.get(main_table, [])
            if name in related_entities:
                body[f"{name.lower()}_id"] = int(id)

        if DEBUG:
            res = await insert(main_table, body, pgpool)
            response2jsonfile(request, "", "requests.json", b, res.status_code)
            return res
        else:
            r = await insert(main_table, body, pgpool)
            return r
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def insert(main_table, payload, pgpool):
    """
    Insert data into the specified main_table using the provided payload.

    Args:
        main_table (str): The name of the main table to insert data into.
        payload (dict): The data payload to be inserted.
        pgpool (asyncpg.pool.Pool): The connection pool to the PostgreSQL database.

    Returns:
        Response: A response object indicating the status of the insertion operation.
    """

    async with pgpool.acquire() as conn:
        async with conn.transaction():
            try:
                _, header = await insert_funcs[main_table](payload, conn)
                return Response(
                    status_code=status.HTTP_201_CREATED,
                    headers={"location": header},
                )
            except ValueError as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"code": 400, "type": "error", "message": str(e)},
                )
            except Exception as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"code": 400, "type": "error", "message": str(e)},
                )


async def insert_record(payload, conn, table):
    """
    Inserts a record into the specified table using the provided payload.

    Args:
        payload (dict): The data to be inserted into the table.
        conn: The database connection object.
        table (str): The name of the table to insert the record into.

    Returns:
        tuple: A tuple containing the insert ID and self link of the inserted record.
    """

    async with conn.transaction():
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
            INSERT INTO sensorthings."{table}" ({keys})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        inserted_id = await conn.fetchval(insert_query, *payload.values())

        if table == "ObservedProperty":
            table = "ObservedProperties"
        else:
            if table != "FeaturesOfInterest":
                table = f"{table}s"
        inserted_self_link = (
            f"{HOSTNAME}{SUBPATH}{VERSION}/{table}({inserted_id})"
        )

        return inserted_id, inserted_self_link


async def insertLocation(payload, conn):
    """
    Inserts location data into the database.

    Args:
        payload (dict or list): The location data to be inserted. It can be a single dictionary or a list of dictionaries.
        conn: The database connection object.

    Returns:
        tuple or single value: If multiple locations are inserted, it returns a tuple containing the inserted location IDs and self-links.
                               If only one location is inserted, it returns a single value containing the location ID and self-link.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            thing_id = None
            new_thing = False
            things = []

            location = payload.get("location")
            if location:
                validate_epsg(location.get("crs"))

            for thing in payload.get("Things", []):
                thing_id = thing.get("@iot.id")
                if thing_id is not None:
                    new_thing = False
                    check_iot_id_in_payload(thing, "Thing")
                else:
                    thing_id, _ = await insertThing(thing, conn)
                    new_thing = True

                things.append((thing_id, new_thing))

            payload.pop("Things", None)

            location_id, location_self_link = await insert_record(
                payload, conn, "Location"
            )

            for thing_id, new_thing in things:
                await manage_thing_location_with_historical_location(
                    conn, thing_id, location_id, new_thing
                )

            return location_id, location_self_link

    except Exception as e:
        format_exception(e)


async def insertThing(payload, conn):
    """
    Inserts a Thing record into the database.

    Args:
        payload (dict): The payload containing the Thing data.
        conn: The database connection object.

    Returns:
        tuple: A tuple containing the Thing ID and selfLink.

    Raises:
        ValueError: If the location_id is not of type `int`.
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            location_id = None
            locations_ids = []

            for location in payload.get("Locations", []):
                location_id = location.get("@iot.id")
                if location_id is None:
                    location_id, _ = await insertLocation(location, conn)
                else:
                    check_iot_id_in_payload(location, "Location")

                locations_ids.append(location_id)

            payload.pop("Locations", None)

            datastreams = payload.pop("Datastreams", [])

            thing_id, thing_selfLink = await insert_record(
                payload, conn, "Thing"
            )

            for location_id in locations_ids:
                await manage_thing_location_with_historical_location(
                    conn, thing_id, location_id, True
                )

            for datastream in datastreams:
                await insertDatastream(datastream, conn, thing_id=thing_id)

            return thing_id, thing_selfLink

    except Exception as e:
        format_exception(e)


async def insertHistoricalLocation(payload, conn):
    """
    Inserts a historical location record into the database.

    Args:
        payload (dict): The payload containing the historical location data.
        conn: The database connection object.

    Returns:
        Tuple: A tuple containing the historical location ID and self link.

    Raises:
        ValueError: If the location ID cannot be deserialized as an integer.
        Exception: If any other error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            new_thing = False
            thing_id = None
            location_id = None
            location_ids = []

            for location in payload.get("Locations", []):
                location_id = location.get("@iot.id")
                if location_id is None:
                    location_id, _ = await insertLocation(location, conn)
                else:
                    check_iot_id_in_payload(location, "Location")

                location_ids.append(location_id)

            payload.pop("Locations", None)

            if "Thing" in payload:
                thing_id = payload["Thing"].get("@iot.id")
                if thing_id is None:
                    thing_id, _ = await insertThing(payload["Thing"], conn)
                    new_thing = True
                payload["thing_id"] = thing_id
                payload.pop("Thing", None)

            handle_datetime_fields(payload)

            historical_location_id, historical_location_selfLink = (
                await insert_record(payload, conn, "HistoricalLocation")
            )

            for location_id in location_ids:
                await manage_thing_location_with_historical_location(
                    conn,
                    thing_id,
                    location_id,
                    new_thing,
                    historical_location_id,
                )

            return historical_location_id, historical_location_selfLink

    except Exception as e:
        format_exception(e)


async def insertSensor(payload, conn):
    """
    Inserts a sensor record into the database.

    Args:
        payload (dict): The payload containing the sensor data.
        conn: The database connection object.

    Returns:
        tuple: A tuple containing the sensor ID and selfLink.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            sensor_id, sensor_selfLink = await insert_record(
                payload, conn, "Sensor"
            )

            datastreams = payload.pop("Datastreams", [])
            if datastreams:
                for datastream in datastreams:
                    await insertDatastream(
                        datastream, conn, sensor_id=sensor_id
                    )

            return sensor_id, sensor_selfLink
    except Exception as e:
        format_exception(e)


async def insertObservedProperty(payload, conn):
    """
    Inserts a new observed property record into the database.

    Args:
        payload (dict): The payload containing the data for the observed property.
        conn: The database connection object.

    Returns:
        tuple: A tuple containing the inserted observed property ID and selfLink.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    try:
        async with conn.transaction():
            observed_property_id, observed_property_selfLink = (
                await insert_record(payload, conn, "ObservedProperty")
            )

            datastreams = payload.pop("Datastreams", [])
            if datastreams:
                for datastream in datastreams:
                    await insertDatastream(
                        datastream,
                        conn,
                        observed_property_id=observed_property_id,
                    )

            return observed_property_id, observed_property_selfLink
    except Exception as e:
        format_exception(e)


async def insertFeaturesOfInterest(payload, conn, datastream_id=None):
    """
    Inserts features of interest into the database.

    Args:
        payload: The payload containing the features of interest data.
        conn: The database connection object.

    Returns:
        A tuple containing the featureofinterest_id and featureofinterest_selfLink.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            feature = payload.get("feature")
            if feature:
                validate_epsg(feature.get("crs"))

            features_of_interest_id, feature_of_interest_self_link = (
                await insert_record(payload, conn, "FeaturesOfInterest")
            )

            if datastream_id is not None:
                await update_datastream_last_foi_id(
                    conn, features_of_interest_id, datastream_id
                )

            observations = payload.pop("Observations", [])
            if observations:
                for observation in observations:
                    await insertObservation(
                        observation,
                        conn,
                        features_of_interest_id=features_of_interest_id,
                    )

            return features_of_interest_id, feature_of_interest_self_link

    except Exception as e:
        format_exception(e)


async def insertDatastream(
    payload, conn, thing_id=None, sensor_id=None, observed_property_id=None
):
    """
    Inserts datastream(s) into the database.

    Args:
        payload (dict or list): The payload containing the datastream(s) to be inserted.
        conn (connection): The database connection object.
        thing_id (int, optional): The ID of the thing associated with the datastream. Defaults to None.

    Returns:
        tuple: A tuple containing the ID and selfLink of the inserted datastream.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            if "@iot.id" in payload:
                check_iot_id_in_payload(payload, "Datastream")

                if thing_id is not None:
                    payload["Thing"] = {"@iot.id": thing_id}
                if sensor_id is not None:
                    payload["Sensor"] = {"@iot.id": sensor_id}
                if observed_property_id is not None:
                    payload["ObservedProperty"] = {
                        "@iot.id": observed_property_id
                    }

                iot_id = payload.pop("@iot.id")
                await updateDatastream(payload, conn, iot_id)

                return (
                    payload["@iot.id"],
                    f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({iot_id})",
                )

            await handle_associations(
                payload, "Thing", thing_id, insertThing, conn
            )
            await handle_associations(
                payload, "Sensor", sensor_id, insertSensor, conn
            )
            await handle_associations(
                payload,
                "ObservedProperty",
                observed_property_id,
                insertObservedProperty,
                conn,
            )

            check_missing_properties(
                payload, ["Thing", "Sensor", "ObservedProperty"]
            )

            observations = []
            if payload.get("Observations"):
                observations = payload.pop("Observations", [])

            handle_datetime_fields(payload)

            datastream_id, datastream_selfLink = await insert_record(
                payload, conn, "Datastream"
            )

            if observations:
                for observation in observations:
                    await insertObservation(observation, conn, datastream_id)

        return datastream_id, datastream_selfLink

    except Exception as e:
        format_exception(e)


async def insertObservation(
    payload, conn, datastream_id=None, features_of_interest_id=None
):
    """
    Inserts observation data into the database.

    Args:
        payload (dict or list): The payload containing the observation(s) to be inserted.
        conn (connection): The database connection object.
        datastream_id (int, optional): The ID of the datastream associated with the observation. Defaults to None.

    Returns:
        tuple: A tuple containing the ID and selfLink of the inserted observation.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

    try:
        async with conn.transaction():
            if "@iot.id" in payload:
                check_iot_id_in_payload(payload, "Observation")

                if datastream_id is not None:
                    payload["Datastream"] = {"@iot.id": datastream_id}

                if features_of_interest_id is not None:
                    payload["FeaturesOfInterest"] = {
                        "@iot.id": features_of_interest_id
                    }

                iot_id = payload.pop("@iot.id")
                await updateObservation(payload, conn, iot_id)

                return (
                    iot_id,
                    f"{HOSTNAME}{SUBPATH}{VERSION}/Observations({iot_id})",
                )

            await handle_associations(
                payload, "Datastream", datastream_id, insertDatastream, conn
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
                    last_foi_id = await conn.fetchval(
                        select_query, payload["datastream_id"]
                    )
                    if last_foi_id != features_of_interest_id:
                        await update_datastream_last_foi_id(
                            conn,
                            features_of_interest_id,
                            payload["datastream_id"],
                        )
                else:
                    features_of_interest_id, _ = (
                        await insertFeaturesOfInterest(
                            payload["FeatureOfInterest"],
                            conn,
                            payload["datastream_id"],
                        )
                    )
                payload.pop("FeatureOfInterest", None)
                payload["featuresofinterest_id"] = features_of_interest_id
            else:
                await generate_feature_of_interest(payload, conn)

            check_missing_properties(
                payload, ["Datastream", "FeaturesOfInterest"]
            )
            handle_datetime_fields(payload)
            handle_result_field(payload)

            if payload.get("phenomenonTime") is None:
                payload["phenomenonTime"] = datetime.now()

            observation_id, observation_self_link = await insert_record(
                payload, conn, "Observation"
            )

            update_query = """
                UPDATE sensorthings."Datastream"
                SET "phenomenonTime" = tstzrange(
                    LEAST($1::timestamptz, lower("phenomenonTime")),
                    GREATEST($1::timestamptz, upper("phenomenonTime")),
                    '[]'
                )
                WHERE id = $2::bigint;
            """
            await conn.execute(
                update_query,
                payload["phenomenonTime"],
                payload["datastream_id"],
            )

            return observation_id, observation_self_link

    except Exception as e:
        format_exception(e)


async def generate_feature_of_interest(payload, conn):
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

    async with conn.transaction():
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

        result = await conn.fetch(query_location_from_thing_datastream)

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

                foi_id, _ = await insert_record(
                    foi_payload, conn, "FeaturesOfInterest"
                )

                update_query = f"""
                    UPDATE sensorthings."Location" 
                    SET "gen_foi_id" = $1::bigint
                    WHERE id = $2::bigint;
                """
                await conn.execute(update_query, foi_id, location_id)

                await update_datastream_last_foi_id(
                    conn, foi_id, payload["datastream_id"]
                )

                payload["featuresofinterest_id"] = foi_id
            else:
                select_query = """
                    SELECT last_foi_id
                    FROM sensorthings."Datastream"
                    WHERE id = $1::bigint;
                """
                last_foi_id = await conn.fetchval(
                    select_query, payload["datastream_id"]
                )

                select_query = """
                    SELECT id
                    FROM sensorthings."Observation"
                    WHERE "datastream_id" = $1::bigint
                    LIMIT 1;
                """
                observation_ids = await conn.fetch(
                    select_query, payload["datastream_id"]
                )

                if last_foi_id is None or not observation_ids:
                    await update_datastream_last_foi_id(
                        conn, gen_foi_id, payload["datastream_id"]
                    )

                payload["featuresofinterest_id"] = gen_foi_id
        else:
            raise ValueError(
                "Can not generate foi for Thing with no locations."
            )


insert_funcs = {
    "Location": insertLocation,
    "Thing": insertThing,
    "HistoricalLocation": insertHistoricalLocation,
    "Sensor": insertSensor,
    "ObservedProperty": insertObservedProperty,
    "FeaturesOfInterest": insertFeaturesOfInterest,
    "Datastream": insertDatastream,
    "Observation": insertObservation,
}


def check_iot_id_in_payload(payload, entity):
    if len(payload) > 1:
        raise ValueError(
            "Invalid payload format: When providing '@iot.id', no other properties should be included."
        )
    if not isinstance(payload["@iot.id"], int):
        raise ValueError(
            f"Expected `{entity} (@iot.id)` to be an `int`, got {type(payload['@iot.id']).__name__}"
        )


async def handle_associations(payload, key, entity_id, insert_func, conn):
    if entity_id is not None:
        payload[f"{key.lower()}_id"] = entity_id
    elif key in payload:
        if "@iot.id" in payload[key]:
            check_iot_id_in_payload(payload[key], key)
            payload[f"{key.lower()}_id"] = payload[key]["@iot.id"]
        else:
            async with conn.transaction():
                entity_id, _ = await insert_func(payload[key], conn)
            payload[f"{key.lower()}_id"] = entity_id
        payload.pop(key, None)


def check_missing_properties(payload, required_properties):
    """
    Check if the payload contains all the required properties.

    Args:
        payload (dict): The payload containing the properties.
        required_properties (list): The list of required properties.

    Raises:
        ValueError: If any of the required properties are missing in the payload.

    Returns:
        None
    """

    missing_properties = [
        f"'{prop}'"
        for prop in required_properties
        if f"{prop.lower()}_id" not in payload
    ]
    if missing_properties:
        raise ValueError(
            f"Missing required properties {', '.join(missing_properties)}"
        )


def format_exception(e):
    """
    Formats an exception by extracting the violating column name from the error message.

    Args:
        e (Exception): The exception to format.

    Returns:
        ValueError: A ValueError object with a formatted error message.
    """
    error_message = str(e)
    column_name_start = error_message.find('"') + 1
    column_name_end = error_message.find('"', column_name_start)
    violating_column = error_message[column_name_start:column_name_end]
    raise ValueError(f"Missing required property '{violating_column}'") from e


def validate_epsg(crs):
    if crs is not None:
        epsg_code = int(crs["properties"].get("name").split(":")[1])
        if epsg_code != EPSG:
            raise ValueError(
                f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
            )


async def manage_thing_location_with_historical_location(
    conn, thing_id, location_id, new_record, historical_location_id=None
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
            await conn.execute(
                """
                    UPDATE sensorthings."Thing_Location"
                    SET "location_id" = $1
                    WHERE "thing_id" = $2;
                """,
                location_id,
                thing_id,
            )

        if historical_location_id is None:
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


async def update_datastream_last_foi_id(conn, foi_id, datastream_id):
    async with conn.transaction():
        update_query = f"""
            UPDATE sensorthings."Datastream" 
            SET last_foi_id = $1::bigint
            WHERE id = $2::bigint;
        """
        await conn.execute(update_query, foi_id, datastream_id)
        await update_datastream_observedArea(conn, datastream_id, foi_id)


async def update_datastream_observedArea(conn, datastream_id, foi_id):
    async with conn.transaction():
        update_query = f"""
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
        await conn.execute(update_query, foi_id, datastream_id)
