import json
import os
import traceback

from app.db.db import get_pool
from app.sta2rest import sta2rest
from app.utils.utils import handle_datetime_fields, handle_result_field
from fastapi.responses import JSONResponse, Response

from fastapi import APIRouter, Depends, Request, status

v1 = APIRouter()

try:
    DEBUG = int(os.getenv("DEBUG"))
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0


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
            if main_table == "Observation" and name == "Datastream":
                body[f"{name.lower()}_id"] = int(id)
            # if main_table == "Location" and name == "Thing":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "Datastream" and name == "Thing":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "Sensor" and name =="Datastream":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "ObservedProperty" and name == "Datastream":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "HistoricalLocation" and name == "Thing":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "Observation" and name == "Datastream":
            #     body[f"{name.lower()}_id"] = int(id)
            # elif main_table == "FeaturesOfInterest" and name == "Observation":
            #     body[f"{name.lower()}_id"] = int(id)

        if DEBUG:
            res = await insert(main_table, body, pgpool)
            response2jsonfile(request, "", "requests.json", b, res.status_code)
            return res
        else:
            return await insert(main_table, body, pgpool)
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
        return Response(
            status_code=status.HTTP_201_CREATED, headers={"location": header}
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
    for key in list(payload.keys()):
        if isinstance(payload[key], dict):
            payload[key] = json.dumps(payload[key])

    keys = ", ".join(f'"{key}"' for key in payload.keys())
    values_placeholders = ", ".join(f"${i+1}" for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."{table}" ({keys}) VALUES ({values_placeholders}) RETURNING (id, "@iot.selfLink")'
    insert_id, insert_selfLink = await conn.fetchval(query, *payload.values())
    return (insert_id, insert_selfLink)


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
            if isinstance(payload, dict):
                payload = [payload]

            location_ids = []
            location_selfLinks = []
            for item in payload:
                for key, value in item.items():
                    if isinstance(value, dict):
                        item[key] = json.dumps(value)

                keys = ", ".join(f'"{key}"' for key in item.keys())
                values_placeholders = ", ".join(
                    f"${i+1}" for i in range(len(item))
                )
                query = f'INSERT INTO sensorthings."Location" ({keys}, "gen_foi_id") VALUES ({values_placeholders}, NULL) RETURNING (id, "@iot.selfLink")'
                location_id, location_selfLink = await conn.fetchval(
                    query, *item.values()
                )
                location_ids.append(location_id)
                location_selfLinks.append(location_selfLink)

            return (
                (location_ids, location_selfLinks)
                if len(location_ids) > 1
                else (location_ids[0], location_selfLinks[0])
            )

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
            new_location = False
            historicallocation_id = None
            if "Locations" in payload:
                if "@iot.id" in payload["Locations"]:
                    location_id = payload["Locations"]["@iot.id"]
                else:
                    location_id, location_selfLink = await insertLocation(
                        payload["Locations"], conn
                    )
                    new_location = True
                if not isinstance(location_id, int):
                    raise ValueError(
                        f"Cannot deserialize value of type `int` from String: {location_id}"
                    )
                payload.pop("Locations")

            datastreams = payload.pop("Datastreams", {})
            thing_id, thing_selfLink = await insert_record(
                payload, conn, "Thing"
            )

            if location_id is not None:
                await conn.execute(
                    'INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id") VALUES ($1, $2)',
                    thing_id,
                    location_id,
                )
                if new_location:
                    queryHistoricalLocations = f'INSERT INTO sensorthings."HistoricalLocation" ("thing_id") VALUES ($1) RETURNING id'
                    historicallocation_id = await conn.fetchval(
                        queryHistoricalLocations, thing_id
                    )
                    if historicallocation_id is not None:
                        await conn.execute(
                            'INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id") VALUES ($1, $2)',
                            location_id,
                            historicallocation_id,
                        )
            if datastreams:
                await insertDatastream(datastreams, conn, thing_id)

            return (thing_id, thing_selfLink)
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
            location_id = None
            if "Locations" in payload:
                for location in payload["Locations"]:
                    if "@iot.id" in location:
                        location_id = location["@iot.id"]
                    else:
                        location_id, location_selfLink = await insertLocation(
                            location, conn
                        )
                    if not isinstance(location_id, int):
                        raise ValueError(
                            f"Cannot deserialize value of type `int` from String: {location_id}"
                        )
                payload.pop("Locations")

            await handle_associations(payload, ["Thing"], conn)
            handle_datetime_fields(payload)
            historicallocation_id, historicallocation_selfLink = (
                await insert_record(payload, conn, "HistoricalLocation")
            )

            if location_id is not None and historicallocation_id is not None:
                await conn.execute(
                    'INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id") VALUES ($1, $2)',
                    location_id,
                    historicallocation_id,
                )

            return (historicallocation_id, historicallocation_selfLink)

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
            return (sensor_id, sensor_selfLink)

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
            observedproperty_id, observedproperty_selfLink = (
                await insert_record(payload, conn, "ObservedProperty")
            )
            return (observedproperty_id, observedproperty_selfLink)

    except Exception as e:
        format_exception(e)


async def insertFeaturesOfInterest(payload, conn):
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
            featureofinterest_id, featureofinterest_selfLink = (
                await insert_record(payload, conn, "FeaturesOfInterest")
            )
            return (featureofinterest_id, featureofinterest_selfLink)

    except Exception as e:
        format_exception(e)


async def insertDatastream(payload, conn, thing_id=None):
    """
    Inserts a datastream record into the database.

    Args:
        payload (dict): The payload containing the datastream information.
        conn: The database connection object.

    Returns:
        tuple: A tuple containing the datastream ID and selfLink.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    try:
        async with conn.transaction():
            datastreams = []
            observations = []
            if isinstance(payload, dict):
                payload = [payload]
            for ds in payload:
                if thing_id:
                    ds["thing_id"] = thing_id
                await handle_associations(
                    ds, ["Thing", "Sensor", "ObservedProperty"], conn
                )
                check_missing_properties(
                    ds, ["Thing", "Sensor", "ObservedProperty"]
                )
                observations.append(ds.pop("Observations", {}))
                handle_datetime_fields(ds)
                for key in list(ds.keys()):
                    if isinstance(ds[key], dict):
                        ds[key] = json.dumps(ds[key])
                datastreams.append(tuple(ds.values()))

            keys = ", ".join(f'"{key}"' for key in ds.keys())
            values_placeholders = ", ".join(
                f"({', '.join(f'${i * len(ds) + j + 1}' for j in range(len(ds)))})"
                for i in range(len(datastreams))
            )
            insert_sql = f"""
            INSERT INTO sensorthings."Datastream" ({keys})
            VALUES {values_placeholders}
            RETURNING id, "@iot.selfLink"
            """

            values = [item for sublist in datastreams for item in sublist]
            result = await conn.fetch(insert_sql, *values)
            for index, row in enumerate(result):
                datastream_id = row["id"]
                if observations:
                    await insertObservation(
                        observations[index], conn, datastream_id
                    )
            datastream_id, datastream_selfLink = (
                result[0]["id"],
                result[0]["@iot.selfLink"],
            )
            return (datastream_id, datastream_selfLink)
    except Exception as e:
        format_exception(e)


async def insertObservation(payload, conn, datastream_id=None):
    """
    Inserts an observation record into the database.

    Args:
        payload (dict): The payload containing the observation data.
        conn (asyncpg.Connection): The database connection.

    Returns:
        tuple: A tuple containing the observation ID and self-link.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    try:
        async with conn.transaction():
            observations = []
            if isinstance(payload, dict):
                payload = [payload]

            for obs in payload:
                if datastream_id:
                    obs["datastream_id"] = datastream_id
                await handle_associations(
                    obs, ["Datastream", "FeatureOfInterest"], conn
                )
                check_missing_properties(
                    obs, ["Datastream", "FeaturesOfInterest"]
                )
                handle_datetime_fields(obs)
                handle_result_field(obs)
                for key in list(obs.keys()):
                    if isinstance(obs[key], dict):
                        obs[key] = json.dumps(obs[key])
                observations.append(tuple(obs.values()))

            keys = ", ".join(f'"{key}"' for key in obs.keys())
            values_placeholders = ", ".join(
                f"({', '.join(f'${i * len(obs) + j + 1}' for j in range(len(obs)))})"
                for i in range(len(observations))
            )

            insert_sql = f"""
            INSERT INTO sensorthings."Observation" ({keys})
            VALUES {values_placeholders}
            RETURNING id, "@iot.selfLink"
            """

            values = [item for sublist in observations for item in sublist]
            result = await conn.fetch(insert_sql, *values)
            if result:
                observation_id, observation_selfLink = (
                    result[0]["id"],
                    result[0]["@iot.selfLink"],
                )
            return (observation_id, observation_selfLink)

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

    if len(result) > 0:
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

            keys = ", ".join(f'"{key}"' for key in foi_payload.keys())
            values_placeholders = ", ".join(
                f"${i+1}" for i in range(len(foi_payload))
            )
            query = f'INSERT INTO sensorthings."FeaturesOfInterest" ({keys}) VALUES ({values_placeholders}) RETURNING id'

            foi_id = await conn.fetchval(query, *foi_payload.values())

            update_query = f"""
                UPDATE sensorthings."Location" 
                SET "gen_foi_id" = $1::bigint 
                WHERE id = $2::bigint
            """
            await conn.execute(update_query, foi_id, location_id)

            payload["featuresofinterest_id"] = foi_id
        else:
            payload["featuresofinterest_id"] = gen_foi_id
    else:
        raise ValueError("Can not generate foi for Thing with no locations.")


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


async def handle_associations(payload, keys, conn):
    """
    Handles associations in the payload by inserting or updating related entities.

    Args:
        payload (dict): The payload containing the associations.
        keys (list): The list of association keys to handle.
        conn: The database connection object.

    Raises:
        ValueError: If the entity_id is not of type `int`.

    Returns:
        None
    """
    for key in keys:
        if key in payload:
            if "@iot.id" in payload[key]:
                entity_id = payload[key]["@iot.id"]
            else:
                if key == "FeatureOfInterest":
                    entity_id, _ = await insertFeaturesOfInterest(
                        payload[key], conn
                    )
                else:
                    entity_id, _ = await insert_funcs[key](payload[key], conn)
            if not isinstance(entity_id, int):
                raise ValueError(
                    f"Cannot deserialize value of type `int` from String: {entity_id}"
                )
            payload.pop(key)
            if key != "FeatureOfInterest":
                payload[f"{key.lower()}_id"] = entity_id
            else:
                payload["featuresofinterest_id"] = entity_id

        else:
            if key == "FeatureOfInterest":
                await generate_feature_of_interest(payload, conn)


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
