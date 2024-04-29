import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from app.utils.utils import create_entity
from fastapi import Depends
from app.db.db import get_pool
import json
from dateutil import parser

v1 = APIRouter()

# Handle POST requests
@v1.api_route("/{path_name:path}", methods=["POST"])
async def catch_all_post(request: Request, path_name: str, pgpool=Depends(get_pool)):
    # Accept only content-type application/json
    if not "content-type" in request.headers or request.headers["content-type"] != "application/json":
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 400,
                "type": "error",
                "message": "Only content-type application/json is supported."
            }
        )

    try:
        full_path = request.url.path
        # parse uri
        result = sta2rest.STA2REST.parse_uri(full_path)
        # get json body
        body = await request.json()
        main_table = result["entity"][0]
        print("PATH", full_path)
        print("BODY", body)
        # result = await create_entity(main_table, body, pgpool)
        result = await insert(main_table, body, pgpool)
        # Return okay
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "code": 200,
                "type": "success",
                "message": result
            }
        )
        
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

async def insert(main_table, payload, pgpool):
    async with pgpool.acquire() as conn:
        async with conn.transaction():
            if main_table == "Location":
                await insertLocation(payload, conn)
            elif main_table == "Thing":
                await insertThing(payload, conn)
            elif main_table == "Sensor":
                await insertSensor(payload, conn)
            elif main_table == "ObservedProperty":
                await insertObservedProperty(payload, conn)
            elif main_table == "Datastream":
                await insertDatastream(payload, conn)
            elif main_table == "Observation":
                await insertObservation(payload, conn)

# LOCATION
async def insertLocation(payload, conn):
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                payload[key] = json.dumps(value)
        keys = ', '.join(f'"{key}"' for key in payload.keys())
        values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
        keys += ', "gen_foi_id"'
        values_placeholders += ", NULL"
        query = f'INSERT INTO sensorthings."Location" ({keys}) VALUES ({values_placeholders}) RETURNING id'
        return await conn.fetchval(query, *payload.values())
    elif isinstance(payload, list):
        for item in payload:
            for key, value in item.items():
                if isinstance(value, dict):
                    item[key] = json.dumps(value)
            keys = ', '.join(f'"{key}"' for key in item.keys())
            values_placeholders = ', '.join(f'${i+1}' for i in range(len(item)))
            query = f'INSERT INTO sensorthings."Location" ({keys}) VALUES ({values_placeholders}) RETURNING id'
            return await conn.fetchval(query, *item.values())
    else:
        print("Payload should be a dictionary or a list of dictionaries.")

# THING
async def insertThing(payload, conn):
    if "Locations" in payload:
        if '@iot.id' in payload["Locations"]:
            location_id = payload["Locations"]["@iot.id"]
        else:
            location_id = await insertLocation(payload["Locations"], conn)
        payload.pop("Locations")
        payload["location_id"] = location_id

    if "Datastreams" in payload:
        datastreams = payload.pop("Datastreams")

    for key, value in payload.items():
        if isinstance(value, dict):
            payload[key] = json.dumps(value)

    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."Thing" ({keys}) VALUES ({values_placeholders}) RETURNING id'

    thing_id = await conn.fetchval(query, *payload.values())

    values = (thing_id, location_id)
    queryHistoricalLocations = f'INSERT INTO sensorthings."HistoricalLocation" ("thing_id", "location_id") VALUES ($1, $2) RETURNING id'
    historicallocations_id = await conn.fetchval(queryHistoricalLocations, *values)

    for ds in datastreams:
        ds["thing_id"] = thing_id
        datastream_id = await insertDatastream(ds, conn)
    return thing_id

# SENSOR
async def insertSensor(payload, conn):
    for key, value in payload.items():
        if isinstance(value, dict):
            payload[key] = json.dumps(value)
    
    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."Sensor" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    return await conn.fetchval(query, *payload.values())

# OBSERVED PROPERTY
async def insertObservedProperty(payload, conn):
    for key, value in payload.items():
        if isinstance(value, dict):
            payload[key] = json.dumps(value)
    
    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."ObservedProperty" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    return await conn.fetchval(query, *payload.values())

# FEATURE OF INTEREST
async def insertFeaturesOfInterest(payload, conn):
    for key, value in payload.items():
        if isinstance(value, dict):
            payload[key] = json.dumps(value)
    
    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."FeaturesOfInterest" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    return await conn.fetchval(query, *payload.values())

# DATASTREAM
async def insertDatastream(payload, conn):    
    if "Thing" in payload:
        if '@iot.id' in payload["Thing"]:
            thing_id = payload["Thing"]["@iot.id"]
        else:
            thing_id = await insertThing(payload["Thing"], conn)
        payload.pop("Thing")
        payload["thing_id"] = thing_id
    
    if "Sensor" in payload:
        if '@iot.id' in payload["Sensor"]:
            sensor_id = payload["Sensor"]["@iot.id"]
        else:
            sensor_id = await insertSensor(payload["Sensor"], conn)
        payload.pop("Sensor")
        payload["sensor_id"] = sensor_id

    if "ObservedProperty" in payload:
        if '@iot.id' in payload["ObservedProperty"]:
            observedproperty_id = payload["ObservedProperty"]["@iot.id"]
        else:
            observedproperty_id = await insertObservedProperty(payload["ObservedProperty"], conn)
        payload.pop("ObservedProperty")
        payload["observedproperty_id"] = observedproperty_id

    if "Observations" in payload:
        observations = payload.pop("Observations")

    for key, value in payload.items():
        if isinstance(value, dict):
            payload[key] = json.dumps(value)
    
    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    query = f'INSERT INTO sensorthings."Datastream" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    
    datastream_id = await conn.fetchval(query, *payload.values())

    for obs in observations:
        obs["datastream_id"] = datastream_id
        observation_id = await insertObservation(obs, conn)
    return datastream_id

# OBSERVATION
async def insertObservation(payload, conn):
    if "FeatureOfInterest" in payload:
        if '@iot.id' in payload["FeatureOfInterest"]:
            featuresofinterest_id = payload["FeatureOfInterest"]["@iot.id"]
        else:
            featuresofinterest_id = await insertFeaturesOfInterest(payload["FeatureOfInterest"], conn)
        payload.pop("FeatureOfInterest")
        payload["featuresofinterest_id"] = featuresofinterest_id
    else:    
        query_location_from_thing_datastream = f'''
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
                sensorthings."Location" l ON t.location_id = l.id
            WHERE
                d.id = {payload["datastream_id"]}
        '''

        result = await conn.fetch(query_location_from_thing_datastream)

        if result:
            location_id, name, description, encoding_type, location, properties, gen_foi_id = result[0]
            if gen_foi_id is None:
                foi_payload = {
                    "name": name,
                    "description": description,
                    "encodingType": encoding_type,
                    "feature": location,
                    "properties": properties
                }

                keys = ', '.join(f'"{key}"' for key in foi_payload.keys())
                values_placeholders = ', '.join(f'${i+1}' for i in range(len(foi_payload)))
                query = f'INSERT INTO sensorthings."FeaturesOfInterest" ({keys}) VALUES ({values_placeholders}) RETURNING id'

                foi_id = await conn.fetchval(query, *foi_payload.values())

                update_query = f'''
                    UPDATE sensorthings."Location" 
                    SET "gen_foi_id" = $1::bigint 
                    WHERE id = $2::bigint
                '''
                await conn.execute(update_query, foi_id, location_id)

                payload["featuresofinterest_id"] = foi_id

            else:
                payload["featuresofinterest_id"] = gen_foi_id

    for key in list(payload.keys()):
        if key == "result":
            result_type, column_name = get_result_type_and_column(payload[key])
            payload[column_name] = payload[key]
            payload["resultType"] = result_type
            payload.pop("result")
        elif "time" in key.lower():
            payload[key] = parser.parse(payload[key])
        elif isinstance(payload[key], dict):
            payload[key] = json.dumps(payload[key])
    
    keys = ', '.join(f'"{key}"' for key in payload.keys())
    values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
    if "resultTime" not in payload:
        keys += ', "resultTime"'
        values_placeholders += ", NULL"
    query = f'INSERT INTO sensorthings."Observation" ({keys}) VALUES ({values_placeholders}) RETURNING id'
    return await conn.fetchval(query, *(value for key, value in payload.items() if key != "resultTime"))

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
