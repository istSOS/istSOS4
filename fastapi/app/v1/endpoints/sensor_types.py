from app.db.db import get_pool
from app.models.sensor import SensorType, Sensor
from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import UJSONResponse


v1 = APIRouter()


################
# SENSOR TYPES #
################
@v1.post("/sensor_types/")
async def create_sensor_type(sensorType: SensorType, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""INSERT INTO public.sensor_type
                (description, metadata)
                VALUES($1, $2)
                RETURNING id""",
                sensorType.description,
                sensorType.metadata,
            )
            if result:
                return UJSONResponse(status_code=status.HTTP_201_CREATED, content=dict(result))
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


@v1.get("/sensor_types/{sensor_type_id}")
async def get_sensor_type(sensor_type_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""SELECT * FROM public.sensor_type
                WHERE id = $1""",
                sensor_type_id,
            )
            if result:
                return UJSONResponse(status_code=status.HTTP_200_OK, content=dict(result))
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


@v1.get("/sensor_types/")
async def get_sensor_types(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM public.sensor_type")
            if result:
                return UJSONResponse(status_code=status.HTTP_200_OK, content=[dict(r) for r in result])
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


@v1.put("/sensor_types/{sensor_type_id}")
async def update_sensor_type(sensor_type_id: int, sensor_type: SensorType, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """ UPDATE public.sensor_type
                SET description = $1, metadata = $2
                WHERE id = $3
                RETURNING *""",
                sensor_type.description,
                sensor_type.metadata,
                sensor_type_id,
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


@v1.delete("/sensor_types/{sensor_type_id}")
async def delete_sensor_type(sensor_type_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """ UPDATE public.sensor
                SET sensor_type_id = null
                WHERE sensor_type_id = $1
                RETURNING *""",
                sensor_type_id,
            )
            result = await conn.fetchrow(
                f"""DELETE FROM public.sensor_type
                WHERE id = $1
                RETURNING *""",
                sensor_type_id,
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