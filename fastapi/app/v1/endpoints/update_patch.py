import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi import status
from app.sta2rest import sta2rest
from fastapi import Depends
from app.db.db import get_pool
import json

v1 = APIRouter()

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
        
        # Check that the column names (key) contains only alphanumeric characters and underscores
        for key in body.keys():
            if not key.isalnum():
                raise Exception(f"Invalid column name: {key}")

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
        if '@iot.id' in payload["Locations"]:
            location_id = payload["Locations"]["@iot.id"]
            query = f'UPDATE sensorthings."Thing_Location" SET location_id = {location_id} WHERE thing_id = ${thing_id} RETURNING ID;'
            await conn.execute(query)
    
    return await update_record(payload, conn, "Thing", thing_id)

async def updateSensor(payload, conn, sensor_id):
    return await update_record(payload, conn, "Sensor", sensor_id)

async def updateObservedProperty(payload, conn, observedproperty_id):
    return await update_record(payload, conn, "ObservedProperty", observedproperty_id)

async def updateFeaturesOfInterest(payload, conn, featuresofinterest_id):
    return await update_record(payload, conn, "FeaturesOfInterest", featuresofinterest_id)

async def updateDatastream(payload, conn, datastream_id):
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
