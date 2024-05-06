import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi import status
from app.sta2rest import sta2rest
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
        return await insert(main_table, body, pgpool)
    except Exception as e:
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
            try:
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
            except ValueError as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "code": 400,
                        "type": "error",
                        "message": str(e)
                    }
                )
            except Exception as e:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={
                        "code": 400,
                        "type": "error",
                        "message": str(e)
                    }
                )
        return Response(status_code=status.HTTP_201_CREATED)

# LOCATION
async def insertLocation(payload, conn):
    try:
        if isinstance(payload, dict):
            payload = [payload]  # Convert dict to list for consistent processing
        elif not isinstance(payload, list):
            raise ValueError("Payload must be a dictionary or a list of dictionaries")

        location_ids = []
        for item in payload:
            for key, value in item.items():
                if isinstance(value, dict):
                    item[key] = json.dumps(value)

            keys = ', '.join(f'"{key}"' for key in item.keys())
            values_placeholders = ', '.join(f'${i+1}' for i in range(len(item)))
            query = f'INSERT INTO sensorthings."Location" ({keys}, "gen_foi_id") VALUES ({values_placeholders}, NULL) RETURNING id'
            location_id = await conn.fetchval(query, *item.values())
            location_ids.append(location_id)

        return location_ids if len(location_ids) > 1 else location_ids[0]

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e

# THING
async def insertThing(payload, conn):
    location_id = None
    location_exist = False
    historicallocation_id = None
    if "Locations" in payload:
        if '@iot.id' in payload["Locations"]:
            location_id = payload["Locations"]["@iot.id"]
            location_exist = True
        else:
            location_id = await insertLocation(payload["Locations"], conn)
        if not isinstance(location_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {location_id}")
        payload.pop("Locations")

    datastreams = payload.pop("Datastreams", {})

    try:
        async with conn.transaction():
            for key, value in payload.items():
                if isinstance(value, dict):
                    payload[key] = json.dumps(value)

            keys = ', '.join(f'"{key}"' for key in payload.keys())
            values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
            query = f'INSERT INTO sensorthings."Thing" ({keys}) VALUES ({values_placeholders}) RETURNING id'
            thing_id = await conn.fetchval(query, *payload.values())

            if location_id:
                await conn.execute(
                    'INSERT INTO sensorthings."Thing_Location" ("thing_id", "location_id") VALUES ($1, $2)',
                    thing_id, location_id
                )
                if not location_exist:
                    queryHistoricalLocations = f'INSERT INTO sensorthings."HistoricalLocation" ("thing_id") VALUES ($1) RETURNING id'
                    historicallocation_id = await conn.fetchval(queryHistoricalLocations, thing_id)
                    if historicallocation_id is not None:
                        await conn.execute(
                            'INSERT INTO sensorthings."Location_HistoricalLocation" ("location_id", "historicallocation_id") VALUES ($1, $2)',
                            location_id, historicallocation_id
                        )

            for ds in datastreams:
                ds["thing_id"] = thing_id
                datastream_id = await insertDatastream(ds, conn)
    except Exception as e:
        raise ValueError(f"{str(e)}") from e

# SENSOR
async def insertSensor(payload, conn):
    try:
        for key, value in payload.items():
            if isinstance(value, dict):
                payload[key] = json.dumps(value)
        
        keys = ', '.join(f'"{key}"' for key in payload.keys())
        values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
        query = f'INSERT INTO sensorthings."Sensor" ({keys}) VALUES ({values_placeholders}) RETURNING id'
        sensor_id = await conn.fetchval(query, *payload.values())
        return sensor_id

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e

# OBSERVED PROPERTY
async def insertObservedProperty(payload, conn):
    try:
        for key, value in payload.items():
            if isinstance(value, dict):
                payload[key] = json.dumps(value)
        
        keys = ', '.join(f'"{key}"' for key in payload.keys())
        values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
        query = f'INSERT INTO sensorthings."ObservedProperty" ({keys}) VALUES ({values_placeholders}) RETURNING id'
        observedproperty_id = await conn.fetchval(query, *payload.values())
        return observedproperty_id

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e


# FEATURE OF INTEREST
async def insertFeaturesOfInterest(payload, conn):
    try:
        for key, value in payload.items():
            if isinstance(value, dict):
                payload[key] = json.dumps(value)
        
        keys = ', '.join(f'"{key}"' for key in payload.keys())
        values_placeholders = ', '.join(f'${i+1}' for i in range(len(payload)))
        query = f'INSERT INTO sensorthings."FeaturesOfInterest" ({keys}) VALUES ({values_placeholders}) RETURNING id'
        featureofinterest_id = await conn.fetchval(query, *payload.values())
        return featureofinterest_id

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e


# DATASTREAM
async def insertDatastream(payload, conn):    
    if "Thing" in payload:  
        if '@iot.id' in payload["Thing"]:
            thing_id = payload["Thing"]["@iot.id"]
        else:
            thing_id = await insertThing(payload["Thing"], conn)
        if not isinstance(thing_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {thing_id}")
        payload.pop("Thing")
        payload["thing_id"] = thing_id
    
    if "Sensor" in payload:
        if '@iot.id' in payload["Sensor"]:
            sensor_id = payload["Sensor"]["@iot.id"]
        else:
            sensor_id = await insertSensor(payload["Sensor"], conn)
        if not isinstance(sensor_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {sensor_id}")
        payload.pop("Sensor")
        payload["sensor_id"] = sensor_id

    if "ObservedProperty" in payload:
        if '@iot.id' in payload["ObservedProperty"]:
            observedproperty_id = payload["ObservedProperty"]["@iot.id"]
        else:
            observedproperty_id = await insertObservedProperty(payload["ObservedProperty"], conn)
        if not isinstance(observedproperty_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {observedproperty_id}")
        payload.pop("ObservedProperty")
        payload["observedproperty_id"] = observedproperty_id

    missing_properties = []
    if "thing_id" not in payload:
        missing_properties.append("'Thing'")
    if "sensor_id" not in payload:
        missing_properties.append("'Sensor'")
    if "observedproperty_id" not in payload:
        missing_properties.append("'ObservedProperty'")
    if missing_properties:
        missing_str = ', '.join(missing_properties)
        raise ValueError(f"Missing required properties {missing_str}")

    observations = payload.pop("Observations", {})

    try:
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

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e

# OBSERVATION
async def insertObservation(payload, conn):
    if "Datastream" in payload:
        if '@iot.id' in payload["Datastream"]:
            datastream_id = payload["Datastream"]["@iot.id"]
        else:
            datastream_id = await insertDatastream(payload["Datastream"], conn)
        if not isinstance(datastream_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {datastream_id}")
        payload.pop("Datastream")
        payload["datastream_id"] = datastream_id

    if "FeatureOfInterest" in payload:
        if '@iot.id' in payload["FeatureOfInterest"]:
            featuresofinterest_id = payload["FeatureOfInterest"]["@iot.id"]
        else:
            featuresofinterest_id = await insertFeaturesOfInterest(payload["FeatureOfInterest"], conn)
        if not isinstance(featuresofinterest_id, int):
            raise ValueError(f"Cannot deserialize value of type `int` from String: {featuresofinterest_id}")
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
                sensorthings."Thing_Location" tl ON tl.thing_id = t.id
            JOIN
                sensorthings."Location" l ON l.ID = tl.location_id
            WHERE
                d.id = {payload["datastream_id"]}
        '''

        result = await conn.fetch(query_location_from_thing_datastream)

        if len(result) > 0:
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
        else:
            raise ValueError("Can not generate foi for Thing with no locations.")
    
    missing_properties = []
    if "datastream_id" not in payload:
        missing_properties.append("'Datastream'")
    if "featuresofinterest_id" not in payload:
        missing_properties.append("'Feature Of Interest'")
    if missing_properties:
        missing_str = ', '.join(missing_properties)
        raise ValueError(f"Missing required properties {missing_str}")

    try:
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
        query = f'INSERT INTO sensorthings."Observation" ({keys}) VALUES ({values_placeholders}) RETURNING id'
        
        observation_id = await conn.fetchval(query, *payload.values())
        return observation_id

    except Exception as e:
        error_message = str(e)
        column_name_start = error_message.find('"') + 1
        column_name_end = error_message.find('"', column_name_start)
        violating_column = error_message[column_name_start:column_name_end]
        raise ValueError(f"Missing required property '{violating_column}'") from e

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
