import json
import traceback
from datetime import datetime

import redis
from app import DEBUG, EPSG, HOSTNAME, REDIS_CACHE_EXPIRATION, SUBPATH, VERSION
from app.db.db import get_pool
from app.sta2rest import sta2rest
from app.utils.utils import handle_datetime_fields, handle_result_field
from fastapi.responses import JSONResponse, Response

from fastapi import APIRouter, Depends, Request, status

v1 = APIRouter()

try:
    DEBUG = DEBUG
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0

ALLOWED_KEYS = {
    "Location": {
        "Things",
    },
    "Thing": {
        "Locations",
        "Datastreams",
    },
    "HistoricalLocation": {
        "Thing",
        "Locations",
    },
    "Sensor": {
        "Datastreams",
    },
    "ObservedProperty": {
        "Datastreams",
    },
    "FeaturesOfInterest": {
        "Observations",
    },
    "Datastream": {
        "Thing",
        "Sensor",
        "ObservedProperty",
        "Observations",
    },
    "Observation": {
        "FeatureOfInterest",
    },
}
# for redis
# Redis client bound to single connection (no auto reconnection).
redis = redis.Redis(host="redis", port=6379, db=0)


def remove_cache(path):
    """
    Remove the cache for the specified path.

    Args:
        path (str): The path to remove the cache for.

    Returns:
        None
    """
    # Pattern da cercare nelle chiavi (ad esempio 'testop')
    pattern = "*{}*".format(path)

    # Itera su tutte le chiavi che corrispondono al pattern
    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=pattern)
        if keys:
            # Cancella le chiavi trovate
            redis.delete(*keys)
        if cursor == 0:
            break


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
            if main_table == "Observation" and name == "Datastream":
                if isinstance(body, dict):
                    body = [body]
                for e in body:
                    e[f"{name.lower()}_id"] = int(id)
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
            r = await insert(main_table, body, pgpool)
            allowed_keys = ALLOWED_KEYS.get(main_table, set())
            for key in allowed_keys:
                if key in body:
                    remove_cache(key)
            remove_cache(full_path.split("/")[-1])
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
    query = f'INSERT INTO sensorthings."{table}" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    insert_id = await conn.fetchval(query, *payload.values())
    if table == "ObservedProperty":
        table = "ObservedProperties"
    else:
        table = f"{table}s"
    insert_selfLink = f"{HOSTNAME}{SUBPATH}{VERSION}/{table}({insert_id})"
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
                    if key == "location":
                        crs = item[key].get("crs")
                        if crs is not None:
                            epsg_code = int(
                                crs["properties"].get("name").split(":")[1]
                            )
                            if epsg_code != EPSG:
                                raise ValueError(
                                    f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
                                )
                    if isinstance(value, dict):
                        item[key] = json.dumps(value)

                keys = ", ".join(f'"{key}"' for key in item.keys())
                values_placeholders = ", ".join(
                    (
                        f"${i+1}"
                        if key != "location"
                        else f"ST_GeomFromGeoJSON(${i+1})"
                    )
                    for i, key in enumerate(item.keys())
                )
                query = f'INSERT INTO sensorthings."Location" ({keys}, "gen_foi_id") VALUES ({values_placeholders}, NULL) RETURNING id'
                location_id = await conn.fetchval(query, *item.values())
                location_selfLink = (
                    f"{HOSTNAME}{SUBPATH}{VERSION}/Locations({location_id})"
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
                    location_id, _ = await insertLocation(
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
            for key in list(payload.keys()):
                if key == "feature":
                    crs = payload[key].get("crs")
                    if crs is not None:
                        epsg_code = int(
                            crs["properties"].get("name").split(":")[1]
                        )
                        if epsg_code != EPSG:
                            raise ValueError(
                                f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
                            )
                if isinstance(payload[key], dict):
                    payload[key] = json.dumps(payload[key])

            keys = ", ".join(f'"{key}"' for key in payload.keys())
            values_placeholders = ", ".join(
                (
                    f"${i+1}"
                    if key != "feature"
                    else f"ST_GeomFromGeoJSON(${i+1})"
                )
                for i, key in enumerate(payload.keys())
            )
            query = f'INSERT INTO sensorthings."FeaturesOfInterest" ({keys}) VALUES ({values_placeholders}) RETURNING id'
            featureofinterest_id = await conn.fetchval(
                query, *payload.values()
            )
            featureofinterest_selfLink = f"{HOSTNAME}{SUBPATH}{VERSION}/FeaturesOfInterest({featureofinterest_id})"
            return (featureofinterest_id, featureofinterest_selfLink)

    except Exception as e:
        format_exception(e)


async def insertDatastream(payload, conn, thing_id=None):
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
            if isinstance(payload, dict):
                payload = [payload]

            datastreams = []
            observations = []
            all_keys = set()

            for ds in payload:
                if thing_id:
                    ds["thing_id"] = thing_id

                await handle_associations(
                    ds, ["Thing", "Sensor", "ObservedProperty"], conn
                )
                check_missing_properties(
                    ds, ["Thing", "Sensor", "ObservedProperty"]
                )

                if "Observations" in ds:
                    observations.append(ds.pop("Observations", {}))
                else:
                    observations.append([])

                handle_datetime_fields(ds)

                for key, value in ds.items():
                    if isinstance(value, dict):
                        ds[key] = json.dumps(value)
                    all_keys.add(key)

            all_keys = list(all_keys)
            for ds in payload:
                ds_tuple = []
                for key in all_keys:
                    ds_tuple.append(ds.get(key))
                datastreams.append(tuple(ds_tuple))

            keys = ", ".join(f'"{key}"' for key in all_keys)
            values_placeholders = ", ".join(
                f"({', '.join(f'${i * len(all_keys) + j + 1}' for j in range(len(all_keys)))})"
                for i in range(len(datastreams))
            )

            insert_sql = f"""
            INSERT INTO sensorthings."Datastream" ({keys})
            VALUES {values_placeholders}
            RETURNING id
            """
            values = [
                value for datastream in datastreams for value in datastream
            ]
            result = await conn.fetch(insert_sql, *values)
            for index, row in enumerate(result):
                datastream_id = row["id"]
                if observations[index]:
                    await insertObservation(
                        observations[index], conn, datastream_id
                    )
            datastream_id = result[0]["id"]
            datastream_selfLink = (
                f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({datastream_id})"
            )
            return (datastream_id, datastream_selfLink)

    except Exception as e:
        format_exception(e)


async def insertObservation(payload, conn, datastream_id=None):
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
            if isinstance(payload, dict):
                payload = [payload]

            observations = []

            all_keys = set()

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

                if obs.get("phenomenonTime") is None:
                    obs["phenomenonTime"] = datetime.now()

                for key, value in obs.items():
                    if isinstance(value, dict):
                        obs[key] = json.dumps(value)
                    all_keys.add(key)

            all_keys = list(all_keys)

            for obs in payload:
                obs_tuple = []
                for key in all_keys:
                    obs_tuple.append(obs.get(key))
                observations.append(tuple(obs_tuple))

            keys = ", ".join(f'"{key}"' for key in all_keys)
            values_placeholders = ", ".join(
                f"({', '.join(f'${i * len(all_keys) + j + 1}' for j in range(len(all_keys)))})"
                for i in range(len(observations))
            )

            insert_sql = f"""
            INSERT INTO sensorthings."Observation" ({keys})
            VALUES {values_placeholders}
            RETURNING id, "phenomenonTime", datastream_id
            """

            values = [
                value for observation in observations for value in observation
            ]
            result = await conn.fetch(insert_sql, *values)

            phenomenon_times = [record["phenomenonTime"] for record in result]

            update_sql = """
                UPDATE sensorthings."Datastream"
                SET "phenomenonTime" = tstzrange(
                    LEAST($1::timestamptz, lower("phenomenonTime")),
                    GREATEST($2::timestamptz, upper("phenomenonTime")),
                    '[]'
                )
                WHERE id = $3::bigint
            """
            await conn.execute(
                update_sql,
                min(phenomenon_times),
                max(phenomenon_times),
                result[0]["datastream_id"],
            )
            observation_id = result[0]["id"]
            observation_selfLink = (
                f"{HOSTNAME}{SUBPATH}{VERSION}/Observations({observation_id})"
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
