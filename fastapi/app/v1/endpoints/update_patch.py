import json
import traceback

from app import DEBUG, EPSG, VERSIONING
from app.db.asyncpg_db import get_pool
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

# Define allowed keys for each main table
ALLOWED_KEYS = {
    "Location": {
        "name",
        "description",
        "encodingType",
        "location",
        "properties",
        "Things",
    },
    "Thing": {
        "name",
        "description",
        "properties",
        "Locations",
        "Datastreams",
    },
    "HistoricalLocation": {
        "time",
        "Thing",
        "Locations",
    },
    "Sensor": {
        "name",
        "description",
        "encodingType",
        "metadata",
        "properties",
        "Datastreams",
    },
    "ObservedProperty": {
        "name",
        "definition",
        "description",
        "properties",
        "Datastreams",
    },
    "FeaturesOfInterest": {
        "name",
        "description",
        "encodingType",
        "feature",
        "properties",
        "Observations",
    },
    "Datastream": {
        "name",
        "description",
        "unitOfMeasurement",
        "observationType",
        "observedArea",
        "phenomenonTime",
        "resultTime",
        "properties",
        "Thing",
        "Sensor",
        "ObservedProperty",
        "Observations",
    },
    "Observation": {
        "phenomenonTime",
        "result",
        "resultTime",
        "resultQuality",
        "validTime",
        "parameters",
        "Datastream",
        "FeatureOfInterest",
    },
}


@v1.api_route("/{path_name:path}", methods=["PATCH"])
async def catch_all_update(
    request: Request, path_name: str, pgpool=Depends(get_pool)
):
    """
    Handle PATCH requests for updating entities.

    Args:
        request (Request): The incoming request object.
        path_name (str): The path name extracted from the URL.
        pgpool: The database connection pool.

    Returns:
        Response: The HTTP response.

    Raises:
        Exception: If no entity name or entity id is provided.
        Exception: If there are invalid keys in the payload for a specific entity.
        Exception: If an error occurs during the update process.
    """
    try:
        full_path = request.url.path
        result = sta2rest.STA2REST.parse_uri(full_path)

        # Validate entity name and id
        name, id = result["entity"]
        if not name or not id:
            raise Exception(
                f"No {'entity name' if not name else 'entity id'} provided"
            )

        body = await request.json()

        if DEBUG:
            try:
                print(f"PATCH body {name}", body)
                import copy

                b = copy.deepcopy(body)
            except:
                b = ""

        if VERSIONING:
            ALLOWED_KEYS["Commit"] = {"message", "author", "encodingType"}
            ALLOWED_KEYS[name].add("Commit")

        if name in ALLOWED_KEYS:
            allowed_keys = ALLOWED_KEYS[name]
            invalid_keys = [
                key for key in body.keys() if key not in allowed_keys
            ]
            if invalid_keys:
                raise Exception(
                    f"Invalid keys in payload for {name}: {', '.join(invalid_keys)}"
                )

        if not body:
            if DEBUG:
                response2jsonfile(request, "", "requests.json", "")
            return Response(status_code=status.HTTP_200_OK)
        if DEBUG:
            r = await update(name, int(id), body, pgpool)
            response2jsonfile(request, "", "requests.json", b, r.status_code)
            return r
        else:
            r = await update(name, int(id), body, pgpool)
            return r
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def update(main_table, record_id, payload, pgpool):
    """
    Update function for the specified main_table.

    Args:
        main_table (str): The name of the main table to update.
        record_id (int): The ID of the record to update.
        payload (dict): The payload containing the updated data.
        pgpool (asyncpg.pool.Pool): The connection pool to the PostgreSQL database.

    Returns:
        Response: A FastAPI Response object with the appropriate status code.

    Raises:
        JSONResponse: A FastAPI JSONResponse object with the appropriate status code and error message.
    """

    update_funcs = {
        "Location": updateLocation,
        "Thing": updateThing,
        "HistoricalLocation": updateHistoricalLocation,
        "Sensor": updateSensor,
        "ObservedProperty": updateObservedProperty,
        "FeaturesOfInterest": updateFeaturesOfInterest,
        "Datastream": updateDatastream,
        "Observation": updateObservation,
    }

    async with pgpool.acquire() as conn:
        async with conn.transaction():
            try:
                await update_funcs[main_table](payload, conn, record_id)
                return Response(status_code=status.HTTP_200_OK)
            except Exception as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"code": 400, "type": "error", "message": str(e)},
                )


async def update_record(payload, conn, table, record_id):
    """
    Update a record in the specified table with the given payload.

    Args:
        payload (dict): The data to update the record with.
        conn: The database connection object.
        table (str): The name of the table to update the record in.
        record_id (int): The ID of the record to update.

    Returns:
        int: The ID of the updated record.

    Raises:
        JSONResponse: If no entity is found for the given ID or if an internal server error occurs.
    """
    async with conn.transaction():
        if VERSIONING:
            query = f"""
                SELECT id
                FROM sensorthings."{table}"
                WHERE id = {record_id};
            """
            selected_id = await conn.fetchval(query)
            if selected_id:
                payload["commit_id"] = await insertCommit(
                    payload["Commit"], conn
                )
                payload.pop("Commit")

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
        if table == "Observation":
            query = f"""
                UPDATE sensorthings."{table}"
                SET {set_clause}
                WHERE id = {record_id}
                RETURNING id, "phenomenonTime", "datastream_id";
            """
            res = await conn.fetchrow(query, *payload.values())
            updated_id = res["id"]
            if res:
                obs_phenomenon_time = res["phenomenonTime"]
                datastream_id = res["datastream_id"]

                datastream_query = """
                    SELECT "phenomenonTime"
                    FROM sensorthings."Datastream"
                    WHERE id = $1;
                """
                datastream_phenomenon_time = await conn.fetchval(
                    datastream_query, datastream_id
                )
                if datastream_phenomenon_time:
                    if (
                        obs_phenomenon_time < datastream_phenomenon_time.lower
                        or obs_phenomenon_time
                        > datastream_phenomenon_time.upper
                    ):
                        new_lower_bound = min(
                            obs_phenomenon_time,
                            datastream_phenomenon_time.lower,
                        )
                        new_upper_bound = max(
                            obs_phenomenon_time,
                            datastream_phenomenon_time.upper,
                        )
                        update_datastream_query = """
                            UPDATE sensorthings."Datastream"
                            SET "phenomenonTime" = tstzrange($1, $2, '[]')
                            WHERE id = $3;
                        """
                        await conn.execute(
                            update_datastream_query,
                            new_lower_bound,
                            new_upper_bound,
                            datastream_id,
                        )

            if payload.get("featuresofinterest_id"):
                await update_datastream_observedArea(conn, datastream_id)
        else:
            query = f"""
                UPDATE sensorthings."{table}"
                SET {set_clause}
                WHERE id = {record_id}
                RETURNING id;
            """
            updated_id = await conn.fetchval(query, *payload.values())

            if table == "FeaturesOfInterest":
                query = """
                    SELECT DISTINCT datastream_id
                    FROM sensorthings."Observation"
                    WHERE "featuresofinterest_id" = $1;
                """
                datastream_ids = await conn.fetch(query, updated_id)
                for ds in datastream_ids:
                    ds = ds["datastream_id"]
                    await update_datastream_observedArea(conn, ds)


async def updateLocation(payload, conn, location_id):
    """
    Update the location information and associated things in the database.

    Args:
        payload (dict): The payload containing the updated location information.
        conn: The database connection object.
        location_id: The ID of the location to be updated.

    Returns:
        The updated location record.

    Raises:
        Exception: If the payload format is invalid or if there is an error during the update process.
    """

    async with conn.transaction():
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
                check = await conn.fetchval(
                    """
                        UPDATE sensorthings."Thing_Location"
                        SET thing_id = $1
                        WHERE location_id = $2;
                    """,
                    location_id,
                    thing_id,
                )
                if check is None:
                    await conn.execute(
                        """
                            INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                            VALUES ($1, $2)
                            ON CONFLICT ("thing_id", "location_id") DO NOTHING;
                        """,
                        thing_id,
                        location_id,
                    )
                historicallocation_id = await conn.fetchval(
                    """
                        INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                        VALUES ($1)
                        RETURNING id;
                    """,
                    thing_id,
                )
                await conn.execute(
                    """
                        INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING;
                    """,
                    location_id,
                    historicallocation_id,
                )
            payload.pop("Things")

        payload["gen_foi_id"] = None

        if payload["location"]:
            crs = payload["location"].get("crs")
            if crs is not None:
                epsg_code = int(crs["properties"].get("name").split(":")[1])
                if epsg_code != EPSG:
                    raise ValueError(
                        f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
                    )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(payload, conn, "Location", location_id)


async def updateThing(payload, conn, thing_id):
    """
    Update a Thing record in the database.

    Args:
        payload (dict): The payload containing the updated Thing data.
        conn: The database connection object.
        thing_id: The ID of the Thing to be updated.

    Returns:
        The updated Thing record.

    Raises:
        Exception: If the payload contains invalid location format.
    """

    async with conn.transaction():
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
                check = await conn.fetchval(
                    """
                        UPDATE sensorthings."Thing_Location"
                        SET location_id = $1
                        WHERE thing_id = $2;
                    """,
                    location_id,
                    thing_id,
                )
                if check is None:
                    await conn.execute(
                        """
                            INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id")
                            VALUES ($1, $2)
                            ON CONFLICT ("thing_id", "location_id") DO NOTHING;
                        """,
                        thing_id,
                        location_id,
                    )
                historicallocation_id = await conn.fetchval(
                    """
                        INSERT INTO sensorthings."HistoricalLocation" ("thing_id")
                        VALUES ($1)
                        RETURNING id;
                    """,
                    thing_id,
                )
                await conn.execute(
                    """
                        INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id")
                        VALUES ($1, $2)
                        ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING;
                    """,
                    location_id,
                    historicallocation_id,
                )
            payload.pop("Locations")

        await handle_nested_entities(
            payload, conn, thing_id, "Datastreams", "thing_id", "Datastream"
        )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(payload, conn, "Thing", thing_id)


async def updateHistoricalLocation(payload, conn, historicallocation_id):
    """
    Update the historical location with the given payload.

    Args:
        payload (dict): The payload containing the updated information.
        conn: The database connection object.
        historicallocation_id: The ID of the historical location to update.

    Raises:
        Exception: If the payload format is invalid.

    Returns:
        The updated historical location record.
    """

    async with conn.transaction():
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
                check = await conn.fetchval(
                    """
                        UPDATE sensorthings."Location_HistoricalLocation"
                        SET location_id = $1
                        WHERE historicallocation_id = $2;
                    """,
                    location_id,
                    historicallocation_id,
                )
                if check is None:
                    await conn.execute(
                        """
                            INSERT INTO sensorthings."Location_HistoricalLocation" ("historicallocation_id", "location_id")
                            VALUES ($1, $2)
                            ON CONFLICT ("historicallocation_id", "location_id") DO NOTHING;
                        """,
                        historicallocation_id,
                        location_id,
                    )
            payload.pop("Locations")

        handle_datetime_fields(payload)
        handle_associations(payload, ["Thing"])

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(
                payload, conn, "HistoricalLocation", historicallocation_id
            )


async def updateSensor(payload, conn, sensor_id):
    """
    Update a sensor record in the database.

    Args:
        payload (dict): The updated sensor data.
        conn: The database connection object.
        sensor_id (int): The ID of the sensor to be updated.

    Returns:
        dict: The updated sensor record.

    Raises:
        Any exceptions that occur during the update process.
    """

    async with conn.transaction():
        await handle_nested_entities(
            payload, conn, sensor_id, "Datastreams", "sensor_id", "Datastream"
        )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(payload, conn, "Sensor", sensor_id)


async def updateObservedProperty(payload, conn, observedproperty_id):
    """
    Update an ObservedProperty record.

    Args:
        payload (dict): The payload containing the updated data for the ObservedProperty.
        conn: The database connection object.
        observedproperty_id (int): The ID of the ObservedProperty record to update.

    Returns:
        The updated ObservedProperty record.

    Raises:
        Any exceptions that occur during the update process.
    """

    async with conn.transaction():
        await handle_nested_entities(
            payload,
            conn,
            observedproperty_id,
            "Datastreams",
            "observedproperty_id",
            "Datastream",
        )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(
                payload, conn, "ObservedProperty", observedproperty_id
            )


async def updateFeaturesOfInterest(payload, conn, featuresofinterest_id):
    """
    Update the features of interest with the given payload.

    Args:
        payload (dict): The payload containing the updated data.
        conn: The database connection object.
        featuresofinterest_id: The ID of the features of interest to be updated.

    Returns:
        The updated record of the features of interest.
    """

    async with conn.transaction():
        await handle_nested_entities(
            payload,
            conn,
            featuresofinterest_id,
            "Observations",
            "featuresofinterest_id",
            "Observation",
        )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload["feature"]:
            crs = payload["feature"].get("crs")
            if crs is not None:
                epsg_code = int(crs["properties"].get("name").split(":")[1])
                if epsg_code != EPSG:
                    raise ValueError(
                        f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
                    )
        if payload:
            await update_record(
                payload, conn, "FeaturesOfInterest", featuresofinterest_id
            )


async def updateDatastream(payload, conn, datastream_id):
    """
    Update a datastream with the given payload.

    Args:
        payload (dict): The payload containing the updated datastream information.
        conn: The database connection object.
        datastream_id (int): The ID of the datastream to be updated.

    Returns:
        dict: The updated datastream record.

    """

    async with conn.transaction():
        handle_datetime_fields(payload)
        handle_associations(payload, ["Thing", "Sensor", "ObservedProperty"])
        await handle_nested_entities(
            payload,
            conn,
            datastream_id,
            "Observations",
            "datastream_id",
            "Observation",
        )

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(payload, conn, "Datastream", datastream_id)


async def updateObservation(payload, conn, observation_id):
    """
    Update an observation record in the database.

    Args:
        payload (dict): The payload containing the updated observation data.
        conn: The database connection object.
        observation_id (int): The ID of the observation record to update.

    Returns:
        The updated observation record.

    """

    async with conn.transaction():
        handle_datetime_fields(payload)
        handle_result_field(payload)
        handle_associations(payload, ["Datastream", "FeatureOfInterest"])

        if VERSIONING:
            check_commit_versioning(payload)

        if payload:
            await update_record(payload, conn, "Observation", observation_id)


async def handle_nested_entities(
    payload, conn, entity_id, key, field, update_table
):
    """
    Handles nested entities in the payload and updates the corresponding database table.

    Args:
        payload (dict): The payload containing the data to be updated.
        conn (connection): The database connection object.
        entity_id (int): The ID of the entity being updated.
        key (str): The key in the payload that contains the nested entities.
        field (str): The field in the database table to be updated.
        update_table (str): The name of the database table to be updated.

    Raises:
        Exception: If the format of the nested entities is invalid.

    Returns:
        None
    """

    async with conn.transaction():
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
                await conn.execute(
                    f'UPDATE sensorthings."{update_table}" SET {field} = {entity_id} WHERE id = {related_id};'
                )
            payload.pop(key)


def handle_associations(payload, keys):
    """
    Handle associations in the payload dictionary.

    Args:
        payload (dict): The payload dictionary.
        keys (list): The list of keys to check in the payload.

    Raises:
        Exception: If the format of the payload is invalid.

    Returns:
        None
    """

    for key in keys:
        if key in payload:
            if list(payload[key].keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each thing dictionary should contain only the '@iot.id' key."
                )
            if key != "FeatureOfInterest":
                payload[f"{key.lower()}_id"] = payload[key]["@iot.id"]
            else:
                payload["featuresofinterest_id"] = payload[key]["@iot.id"]
            payload.pop(key)


def check_commit_versioning(payload):
    """
    Check the versioning of the payload.

    Args:
        payload (dict): The payload to be checked.

    Raises:
        Exception: If the "Commit" field is missing in the payload.
        Exception: If there are invalid keys in the payload for "Commit".
        Exception: If the "message" field is missing in the "Commit" field.

    """

    if "Commit" not in payload:
        raise Exception("Commit field is required for versioning update.")

    if "Commit" in ALLOWED_KEYS:
        allowed_keys = ALLOWED_KEYS["Commit"]
        invalid_keys = [
            key for key in payload["Commit"].keys() if key not in allowed_keys
        ]
        if invalid_keys:
            raise Exception(
                f'Invalid keys in payload for {payload["Commit"]}: {", ".join(invalid_keys)}'
            )

    if "message" not in payload["Commit"]:
        raise Exception("Commit message is required for versioning update.")

    if "author" not in payload["Commit"]:
        payload["Commit"]["author"] = "anonymous"

    if "encodingType" not in payload["Commit"]:
        payload["Commit"]["encodingType"] = "text/plain"


async def insertCommit(payload, conn):
    async with conn.transaction():
        for key in list(payload.keys()):
            if isinstance(payload[key], dict):
                payload[key] = json.dumps(payload[key])

        keys = ", ".join(f'"{key}"' for key in payload.keys())
        values_placeholders = ", ".join(f"${i+1}" for i in range(len(payload)))
        query = f"""
            INSERT INTO sensorthings."Commit" ({keys})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        return await conn.fetchval(query, *payload.values())


async def update_datastream_observedArea(conn, datastream_id):
    async with conn.transaction():
        query = """
            WITH distinct_features AS (
                SELECT DISTINCT ON (foi.id) foi.feature
                FROM sensorthings."Observation" o, sensorthings."FeaturesOfInterest" foi
                WHERE o.featuresofinterest_id = foi.id AND o.datastream_id = $1
            ),
            aggregated_geometry AS (
                SELECT ST_ConvexHull(ST_Collect(feature)) AS agg_geom
                FROM distinct_features
            )
            UPDATE sensorthings."Datastream"
            SET "observedArea" = (SELECT agg_geom FROM aggregated_geometry)
            WHERE id = $1;
        """
        await conn.execute(query, datastream_id)
