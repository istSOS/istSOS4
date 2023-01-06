from fastapi import FastAPI, Depends, Response, status
from pydantic import BaseModel, Extra
from enum import Enum
import asyncpg
import json
import uuid
from fastapi.responses import UJSONResponse
from datetime import timedelta


class ContactType(str, Enum):
    owner = "owner"
    manufacturer = "manufacturer"
    operator = "operator"


class Contact(BaseModel):
    type: ContactType
    person: str | None = None
    telephone: str | None = None
    fax: str | None = None
    email: str | None = None
    web: str | None = None
    address: str | None = None
    city: str | None = None
    admin_area: str | None = None
    postal_code: str | None = None
    country: str | None = None


class SensorType(BaseModel):
    description: str | None = None
    metadata: str | None = None


class Sensor(BaseModel):
    name: str | None = None
    description: str | None = None
    encoding_type: str | None = None
    sampling_time_resolution: timedelta | None = None
    acquisition_time_resolution: timedelta | None = None
    sensor_type: SensorType | int | None = None
    contact: Contact | int | None = None

    class Config:
        extra = Extra.allow


app = FastAPI()

pgpool: asyncpg.Pool | None = None


async def get_pool():
    global pgpool
    if not pgpool:
        pgpool = await asyncpg.create_pool(dsn='postgresql://admin:admin@database:5432/istsos3')
    return pgpool


@app.get("/contact_types/")
async def get_contact_types(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT enum_range(NULL:: public.contact_type) AS contact_types"
            )
            return UJSONResponse(status_code=status.HTTP_200_OK, content=dict(result))
        pgpool.close()
    except Exception as e:
        return str(e)


@app.post("/contacts/")
async def create_contact(contact: Contact, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                    INSERT INTO public.contact
                    (type, person, telephone, fax, email, web,
                        address, city, admin_area, postal_code, country)
                    VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                """,
                contact.type,
                contact.person,
                contact.telephone,
                contact.fax,
                contact.email,
                contact.web,
                contact.address,
                contact.city,
                contact.admin_area,
                contact.postal_code,
                contact.country,
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.get("/contacts/{contact_id}")
async def get_contact(contact_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                    SELECT * FROM public.contact
                    WHERE id = $1
                """,
                contact_id,
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.get("/contacts/")
async def get_contacts(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetch(
                "SELECT * FROM public.contact"
            )
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.put("/contacts/{contact_id}")
async def update_contact(contact_id: int, contact: Contact,  pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """ UPDATE public.contact
                SET type = $1, person = $2, telephone = $3, fax = $4, email = $5, web = $6, address = $7, city = $8, admin_area = $9, postal_code = $10, country = $11
                WHERE id = $12
                RETURNING id""",
                contact.type,
                contact.person,
                contact.telephone,
                contact.fax,
                contact.email,
                contact.web,
                contact.address,
                contact.city,
                contact.admin_area,
                contact.postal_code,
                contact.country,
                contact_id,
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.delete("/contacts/{contact_id}")
async def delete_contact(contact_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """DELETE FROM public.sensor_contact
                WHERE contact_id = $1
                RETURNING *""",
                contact_id,
            )
            result = await conn.fetchrow(
                f"""DELETE FROM public.contact
                WHERE id = $1
                RETURNING id""",
                contact_id,
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.post("/sensor_types/")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.get("/sensor_types/{sensor_type_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.get("/sensor_types/")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.put("/sensor_types/{sensor_type_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.delete("/sensor_types/{sensor_type_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@app.post("/sensors/")
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
                sensorType_id,
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.get("/sensors/{sensor_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.get("/sensors/")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.put("/sensors/{sensor_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)


@ app.delete("/sensors/{sensor_id}")
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
        pgpool.close()
    except Exception as e:
        return str(e)
