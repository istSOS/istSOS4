import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi import status
from app.sta2rest import sta2rest
from fastapi import Depends
from app.db.db import get_pool
from dateutil import parser
import json

v1 = APIRouter()

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
    "Thing": {"name", "description", "properties", "Locations", "Datastreams"},
    "HistoricalLocation": {"time", "Thing", "Locations"},
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

# Handle UPDATE requests


@v1.api_route("/{path_name:path}", methods=["PATCH"])
async def catch_all_update(request: Request, path_name: str, pgpool=Depends(get_pool)):
    try:
        full_path = request.url.path
        result = sta2rest.STA2REST.parse_uri(full_path)

        # Validate entity name and id
        name, id = result["entity"]
        if not name or not id:
            raise Exception(
                f"No {'entity name' if not name else 'entity id'} provided")

        body = await request.json()

        print(f"BODY PATCH {name}", body)

        ##############################################
        ##############################################
        # Definisci il percorso del file JSON
        file_json = 'requests.json'

        # Leggi il file JSON e salva il contenuto in una variabile
        try:
            with open(file_json, 'r') as file:
                dati = json.load(file)
        except:
            dati = []
        dati.append({
            "path": full_path,
            "method": "PATCH",
            "body": body
        })
        # Risalva i dati JSON modificati nello stesso file
        with open(file_json, 'w') as file:
            json.dump(dati, file, indent=4)
        ##############################################
        ##############################################

        if name in ALLOWED_KEYS:
            allowed_keys = ALLOWED_KEYS[name]
            invalid_keys = [
                key for key in body.keys() if key not in allowed_keys]
            if invalid_keys:
                raise Exception(
                    f"Invalid keys in payload for {name}: {', '.join(invalid_keys)}"
                )

        if not body:
            return Response(status_code=status.HTTP_200_OK)

        return await update(name, int(id), body, pgpool)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )


async def update(main_table, record_id, payload, pgpool):
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
    try:
        async with conn.transaction():
            payload = {
                key: json.dumps(value) if isinstance(value, dict) else value
                for key, value in payload.items()
            }
            set_clause = ", ".join(
                [f'"{key}" = ${i + 1}' for i, key in enumerate(payload.keys())]
            )
            query = f'UPDATE sensorthings."{table}" SET {set_clause} WHERE id = ${len(payload) + 1} RETURNING ID;'
            updated_id = await conn.fetchval(query, *payload.values(), int(record_id))
            if not updated_id:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "code": 404,
                        "type": "error",
                        "message": "No entity found for the given id.",
                    },
                )

            return updated_id
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": 500,
                "type": "error",
                "message": "An internal server error occurred.",
            },
        )


async def updateLocation(payload, conn, location_id):
    if "Things" in payload:
        if isinstance(payload["Things"], dict):
            payload["Things"] = [payload["Things"]]
        for thing in payload["Things"]:
            if not isinstance(thing, dict) or list(thing.keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each thing should be a dictionary with a single key '@iot.id'."
                )
            thing_id = thing["@iot.id"]
            check = await conn.fetchval(
                'UPDATE sensorthings."Thing_Location" SET thing_id = $1 WHERE location_id = $2',
                location_id,
                thing_id,
            )
            if check is None:
                await conn.execute(
                    'INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id") VALUES ($1, $2) ON CONFLICT ("thing_id", "location_id") DO NOTHING',
                    thing_id,
                    location_id,
                )
            historicallocation_id = await conn.fetchval(
                'INSERT INTO sensorthings."HistoricalLocation" ("thing_id") VALUES ($1) RETURNING id',
                thing_id,
            )
            await conn.execute(
                'INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id") VALUES ($1, $2)  ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING',
                location_id,
                historicallocation_id,
            )
        payload.pop("Things")
    return await update_record(payload, conn, "Location", location_id)


async def updateThing(payload, conn, thing_id):
    if "Locations" in payload:
        if isinstance(payload["Locations"], dict):
            payload["Locations"] = [payload["Locations"]]
        for location in payload["Locations"]:
            if not isinstance(location, dict) or list(location.keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each location should be a dictionary with a single key '@iot.id'."
                )
            location_id = location["@iot.id"]
            check = await conn.fetchval(
                'UPDATE sensorthings."Thing_Location" SET location_id = $1 WHERE thing_id = $2',
                location_id,
                thing_id,
            )
            if check is None:
                await conn.execute(
                    'INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id") VALUES ($1, $2) ON CONFLICT ("thing_id", "location_id") DO NOTHING',
                    thing_id,
                    location_id,
                )
            historicallocation_id = await conn.fetchval(
                'INSERT INTO sensorthings."HistoricalLocation" ("thing_id") VALUES ($1) RETURNING id',
                thing_id,
            )
            await conn.execute(
                'INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id") VALUES ($1, $2) ON CONFLICT ("location_id", "historicallocation_id") DO NOTHING',
                location_id,
                historicallocation_id,
            )
        payload.pop("Locations")
    await handle_nested_entities(
        payload, conn, thing_id, "Datastreams", "thing_id", "Datastream"
    )
    return await update_record(payload, conn, "Thing", thing_id)


async def updateHistoricalLocation(payload, conn, historicallocation_id):
    if "Locations" in payload:
        if isinstance(payload["Locations"], dict):
            payload["Locations"] = [payload["Locations"]]
        for location in payload["Locations"]:
            if not isinstance(location, dict) or list(location.keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each location should be a dictionary with a single key '@iot.id'."
                )
            location_id = location["@iot.id"]
            check = await conn.fetchval(
                'UPDATE sensorthings."Location_HistoricalLocation" SET location_id = $1 WHERE historicallocation_id = $2',
                location_id,
                historicallocation_id,
            )
            if check is None:
                await conn.execute(
                    'INSERT INTO sensorthings."Location_HistoricalLocation" ("historicallocation_id", "location_id") VALUES ($1, $2) ON CONFLICT ("historicallocation_id", "location_id") DO NOTHING',
                    historicallocation_id,
                    location_id,
                )
        payload.pop("Locations")
    handle_datetime_fields(payload)
    handle_associations(payload, ["Thing"])
    return await update_record(
        payload, conn, "HistoricalLocation", historicallocation_id
    )


async def updateSensor(payload, conn, sensor_id):
    await handle_nested_entities(
        payload, conn, sensor_id, "Datastreams", "sensor_id", "Datastream"
    )
    return await update_record(payload, conn, "Sensor", sensor_id)


async def updateObservedProperty(payload, conn, observedproperty_id):
    await handle_nested_entities(
        payload,
        conn,
        observedproperty_id,
        "Datastreams",
        "observedproperty_id",
        "Datastream",
    )
    return await update_record(payload, conn, "ObservedProperty", observedproperty_id)


async def updateFeaturesOfInterest(payload, conn, featuresofinterest_id):
    await handle_nested_entities(
        payload,
        conn,
        featuresofinterest_id,
        "Observations",
        "featuresofinterest_id",
        "Observation",
    )
    return await update_record(
        payload, conn, "FeaturesOfInterest", featuresofinterest_id
    )


async def updateDatastream(payload, conn, datastream_id):
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
    return await update_record(payload, conn, "Datastream", datastream_id)


async def updateObservation(payload, conn, observation_id):
    handle_datetime_fields(payload)
    handle_result_field(payload)
    handle_associations(payload, ["Datastream", "FeatureOfInterest"])
    return await update_record(payload, conn, "Observation", observation_id)


async def handle_nested_entities(payload, conn, entity_id, key, field, update_table):
    if key in payload:
        if isinstance(payload[key], dict):
            payload[key] = [payload[key]]
        for item in payload[key]:
            if not isinstance(item, dict) or list(item.keys()) != ["@iot.id"]:
                raise Exception(
                    f"Invalid format: Each item in '{key}' should be a dictionary with a single key '@iot.id'."
                )
            related_id = item["@iot.id"]
            await conn.execute(
                f'UPDATE sensorthings."{update_table}" SET {field} = {entity_id} WHERE id = {related_id};'
            )
        payload.pop(key)


def handle_datetime_fields(payload):
    for key in list(payload.keys()):
        if "time" in key.lower():
            payload[key] = parser.parse(payload[key])


def handle_associations(payload, keys):
    for key in keys:
        if key in payload:
            if list(payload[key].keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each thing dictionary should contain only the '@iot.id' key."
                )
            id_field = f"{key.lower()}_id"
            payload[id_field] = payload[key]["@iot.id"]
            payload.pop(key)


def handle_result_field(payload):
    for key in list(payload.keys()):
        if key == "result":
            result_type, column_name = get_result_type_and_column(payload[key])
            payload[column_name] = payload[key]
            payload["resultType"] = result_type
            payload.pop("result")


def get_result_type_and_column(input_string):
    try:
        value = eval(str(input_string))
    except (SyntaxError, NameError):
        result_type = 0
        column_name = "resultString"
    else:
        if isinstance(value, int):
            result_type = 1
            column_name = "resultInteger"
        elif isinstance(value, float):
            result_type = 2
            column_name = "resultDouble"
        elif isinstance(value, dict):
            result_type = 4
            column_name = "resultJSON"
        else:
            result_type = None
            column_name = None

    if input_string in ["true", "false"]:
        result_type = 3
        column_name = "resultBoolean"

    if result_type is not None:
        return result_type, column_name
    else:
        raise Exception("Cannot cast result to a valid type")
