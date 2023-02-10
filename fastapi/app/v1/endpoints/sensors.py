import json, uuid
from app.db.db import get_pool
from app.models.sensor import SensorType, Sensor
from app.models.contact import Contact
from app.v1.endpoints.contacts import *
from app.v1.endpoints.sensor_types import *
from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import UJSONResponse


v1 = APIRouter()


################
#   SENSORS    #
################
@v1.post("/Sensors/")
async def create_sensor(sensor: Sensor, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            contact_id = None
            sensorType_id = None
            ID = None
            if (type(sensor.contact)) is Contact:
                result = await create_contact(sensor.contact, pgpool)
                contact_id = json.loads(result.body.decode("utf-8"))["id"]
            if (type(sensor.contact)) is int:
                contact_id = sensor.contact
            if (type(sensor.sensor_type)) is SensorType:
                result = await create_sensor_type(sensor.sensor_type, pgpool)
                sensorType_id = json.loads(result.body.decode("utf-8"))["id"]
            if (type(sensor.sensor_type)) is int:
                sensorType_id = sensor.sensor_type

            ID = str(uuid.uuid4())
            for key in dict(sensor):
                if key == "id":
                    ID = sensor.id
            result = await conn.fetchrow(
                f"""INSERT INTO public.sensor
            (id, name, description, encoding_type, sampling_time_resolution,
            acquisition_time_resolution, sensor_type_id)
            VALUES($1, $2, $3, $4, $5, $6, $7)
            RETURNING *""",
                ID,
                sensor.name,
                sensor.description,
                sensor.encoding_type,
                sensor.sampling_time_resolution,
                sensor.acquisition_time_resolution,
                sensorType_id
            )

            tmp_result = result
            if contact_id is not None:
                sensor_id = dict(result)["id"]
                result = await conn.fetchrow(
                    f"""INSERT INTO public.sensor_contact
                (sensor_id, contact_id)
                VALUES($1, $2)
                ON CONFLICT DO NOTHING
                RETURNING *""",
                    sensor_id,
                    contact_id,
                )
            if tmp_result:
                for key in dict(tmp_result):
                    if key == "id":
                        tmp_result = {"id": str(dict(tmp_result)[key])}
                return UJSONResponse(status_code=status.HTTP_201_CREATED, content=tmp_result)
            else:
                result = {
                    "exceptionReport": {
                        "code": "InvalidParameterValue",
                        "locator": "id"
                    }
                }
                return UJSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=result)
    except Exception as e:
        return str(e)


@v1.get("/Sensors/{sensor_id}")
async def get_sensor(sensor_id: str, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""SELECT * FROM public.sensor
                WHERE id::text = $1""",
                sensor_id,
            )
            if result:
                tmp_result = dict(result)
                for key in tmp_result:
                    if key == "id" or key == "sampling_time_resolution" or key == "acquisition_time_resolution":
                        tmp_result[key] = str(tmp_result[key])
                return UJSONResponse(status_code=status.HTTP_200_OK, content=tmp_result)
            else:
                result = {
                    "exceptionReport": {
                        "code": "InvalidParameterValue",
                        "locator": "id"
                    }
                }
                return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
    except Exception as e:
        return str(e)


@v1.get("/Sensors/")
async def get_sensors(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM public.sensor")
            tmp_result = [dict(r) for r in result]
            for key in tmp_result:
                key["id"] = str(key["id"])
                key["sampling_time_resolution"] = str(key["sampling_time_resolution"])
                key["acquisition_time_resolution"] = str(key["acquisition_time_resolution"])

            if result:
                return UJSONResponse(status_code=status.HTTP_200_OK, content=tmp_result)
            else:
                result = {
                    "exceptionReport": {
                        "code": "InvalidParameterValue",
                        "locator": "id"
                    }
                }
                return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
    except Exception as e:
        return str(e)


@v1.put("/Sensors({sensor_id})")
async def update_sensor(sensor_id: str, sensor: Sensor, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            contact_id = None
            sensorType_id = None
            if (type(sensor.contact)) is Contact:
                result = await create_contact(sensor.contact, pgpool)
                contact_id = json.loads(result.body.decode("utf-8"))["id"]
            if (type(sensor.contact)) is int:
                contact_id = sensor.contact
            if (type(sensor.sensor_type)) is SensorType:
                result = await create_sensor_type(sensor.sensor_type, pgpool)
                sensorType_id = json.loads(result.body.decode("utf-8"))["id"]
            if (type(sensor.sensor_type)) is int:
                sensorType_id = sensor.sensor_type
            result = await conn.fetchrow(
                """ UPDATE public.sensor
                SET name = $1, description = $2, encoding_type = $3, sampling_time_resolution = $4, acquisition_time_resolution = $5, sensor_type_id = $6
                WHERE id::text = $7
                RETURNING *""",
                sensor.name,
                sensor.description,
                sensor.encoding_type,
                sensor.sampling_time_resolution,
                sensor.acquisition_time_resolution,
                sensorType_id,
                sensor_id,
            )
            tmp_result = result
            if contact_id is not None:
                sensor_id = dict(result)["id"]
                result = await conn.fetchrow(
                    f"""INSERT INTO public.sensor_contact
                (sensor_id, contact_id)
                VALUES($1, $2)
                ON CONFLICT DO NOTHING
                RETURNING *""",
                    sensor_id,
                    contact_id,
                )

            if tmp_result:
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            else:
                result = {
                    "exceptionReport": {
                        "code": "InvalidParameterValue",
                        "locator": "id"
                    }
                }
                return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
    except Exception as e:
        return str(e)


@v1.delete("/Sensors({sensor_id})")
async def delete_sensor(sensor_id: str, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """DELETE FROM public.sensor_contact
                WHERE sensor_id::text = $1
                RETURNING *""",
                sensor_id,
            )
            result = await conn.fetchrow(
                f"""DELETE FROM public.sensor
                WHERE id::text = $1
                RETURNING *""",
                sensor_id,
            )
            if result:
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            else:
                result = {
                    "exceptionReport": {
                        "code": "InvalidParameterValue",
                        "locator": "id"
                    }
                }
                return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
    except Exception as e:
        return str(e)