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
    "Location": {"name", "description", "encodingType", "location", "properties"},
    "Thing": {"name", "description", "properties", "Locations"},
    "Sensor": {"name", "description", "encodingType", "metadata", "properties"},
    "ObservedProperty": {"name", "definition", "description", "properties"},
    "FeaturesOfInterest": {"name", "description", "encodingType", "feature", "properties"},
    "Datastream": {"name", "description", "unitOfMeasurement", "observationType", "observedArea", "phenomenonTime", "resultTime", "properties", "Thing", "Sensor", "ObservedProperty"},
    "Observation": {"phenomenonTime", "result", "resultTime", "resultQuality", "validTime", "parameters"}
}

# Handle UPDATE requests
@v1.api_route("/{path_name:path}", methods=["PATCH"])
async def catch_all_update(request: Request, path_name: str, pgpool=Depends(get_pool)):
    try:
        full_path = request.url.path
        # Parse URI
        result = sta2rest.STA2REST.parse_uri(full_path)

        # Get main entity
        [name, id] = result["entity"]

        # Validate name and id
        if not name:
            raise Exception("No entity name provided")
    
        if not id:
            raise Exception("No entity id provided")

        body = await request.json()
        
        # Ensure only allowed keys are in the payload
        if name in ALLOWED_KEYS:
            allowed_keys = ALLOWED_KEYS[name]
            for key in body.keys():
                if key not in allowed_keys:
                    raise ValueError(f"Invalid key in payload for {name}: {key}")

        print("BODY PATCH", body)

        if not body:
            return Response(status_code=status.HTTP_200_OK)
        
        return await update(name, id, body, pgpool)

    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": str(e)
            }
        )
    
async def update(main_table, record_id, payload, pgpool):
    async with pgpool.acquire() as conn:
        async with conn.transaction():
            try:
                if main_table == "Location":
                    await updateLocation(payload, conn, record_id)
                elif main_table == "Thing":
                    await updateThing(payload, conn, record_id)
                elif main_table == "Sensor":
                    await updateSensor(payload, conn, record_id)
                elif main_table == "ObservedProperty":
                    await updateObservedProperty(payload, conn, record_id)
                elif main_table == "FeaturesOfInterest":
                    await updateFeaturesOfInterest(payload, conn, record_id)
                elif main_table == "Datastream":
                    await updateDatastream(payload, conn, record_id)
                elif main_table == "Observation":
                    await updateObservation(payload, conn, record_id)
                return Response(status_code=status.HTTP_200_OK)
            except Exception as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "code": 400,
                        "type": "error",
                        "message": str(e)
                    }
                )

async def update_record(payload, conn, table, record_id):
    try:
        async with conn.transaction():
            # Convert dictionary values to JSON strings
            payload = {key: json.dumps(value) if isinstance(value, dict) else value for key, value in payload.items()}
            
            # Prepare query dynamically
            set_clause = ', '.join([f'"{key}" = ${i + 1}' for i, key in enumerate(payload.keys())])
            query = f'UPDATE sensorthings."{table}" SET {set_clause} WHERE id = ${len(payload) + 1} RETURNING ID;'
            
            # Execute the query
            updated_id = await conn.fetchval(query, *payload.values(), int(record_id))
            
            # Check if the update was successful
            if updated_id is None:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "code": 404,
                        "type": "error",
                        "message": "No entity found for the given id."
                    }
                )
            
            return updated_id
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": 500,
                "type": "error",
                "message": "An internal server error occurred."
            }
        )

async def updateLocation(payload, conn, location_id):
    return await update_record(payload, conn, "Location", location_id)
    
async def updateThing(payload, conn, thing_id):
    if "Locations" in payload:
        # Ensure "Locations" is a list
        if not isinstance(payload["Locations"], list):
            raise ValueError("Invalid format: 'Locations' should be a list.")

        # Validate each item in the "Locations" list
        for location in payload["Locations"]:
            if not isinstance(location, dict):
                raise ValueError("Invalid format: Each location should be a dictionary.")
            
            # Check that the only key is '@iot.id'
            if list(location.keys()) != ['@iot.id']:
                raise ValueError("Invalid format: Each location dictionary should contain only the '@iot.id' key.")

            location_id = location['@iot.id']
            query = f'UPDATE sensorthings."Thing_Location" SET location_id = {location_id} WHERE thing_id = {thing_id};'
            await conn.execute(query)
        payload.pop("Locations")

    return await update_record(payload, conn, "Thing", thing_id)

async def updateSensor(payload, conn, sensor_id):
    return await update_record(payload, conn, "Sensor", sensor_id)

async def updateObservedProperty(payload, conn, observedproperty_id):
    return await update_record(payload, conn, "ObservedProperty", observedproperty_id)

async def updateFeaturesOfInterest(payload, conn, featuresofinterest_id):
    return await update_record(payload, conn, "FeaturesOfInterest", featuresofinterest_id)

async def updateDatastream(payload, conn, datastream_id):
    for key in list(payload.keys()):
        if "time" in key.lower():
            payload[key] = parser.parse(payload[key])

    if "Thing" in payload:  
        if '@iot.id' in payload["Thing"]:
            thing_id = payload["Thing"]["@iot.id"]
        payload.pop("Thing")
        payload["thing_id"] = thing_id

    if "Sensor" in payload:
        if '@iot.id' in payload["Sensor"]:
            sensor_id = payload["Sensor"]["@iot.id"]
        payload.pop("Sensor")
        payload["sensor_id"] = sensor_id

    if "ObservedProperty" in payload:
        if '@iot.id' in payload["ObservedProperty"]:
            observedproperty_id = payload["ObservedProperty"]["@iot.id"]
        payload.pop("ObservedProperty")
        payload["observedproperty_id"] = observedproperty_id

    return await update_record(payload, conn, "Datastream", datastream_id)

async def updateObservation(payload, conn, observation_id):
    for key in list(payload.keys()):
        if key == "result":
            result_type, column_name = get_result_type_and_column(payload[key])
            payload[column_name] = payload[key]
            payload["resultType"] = result_type
            payload.pop("result")
        elif "time" in key.lower():
            payload[key] = parser.parse(payload[key])

    if "Datastream" in payload:
        if '@iot.id' in payload["Datastream"]:
            datastream_id = payload["Datastream"]["@iot.id"]
        payload.pop("Datastream")
        payload["datastream_id"] = datastream_id

    if "FeatureOfInterest" in payload:
        if '@iot.id' in payload["FeatureOfInterest"]:
            featuresofinterest_id = payload["FeatureOfInterest"]["@iot.id"]
        payload.pop("FeatureOfInterest")
        payload["featuresofinterest_id"] = featuresofinterest_id

    return await update_record(payload, conn, "Observation", observation_id)

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

    if input_string == "true" or input_string == "false":
        result_type = 3
        column_name = "resultBoolean"

    if result_type is not None:
        return result_type, column_name
    else:
        raise Exception("Cannot cast result to a valid type")
