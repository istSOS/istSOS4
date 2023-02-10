from app.db.db import get_pool
from app.models.observed_property import ObservedProperty
from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import UJSONResponse


v1 = APIRouter()

###########################
#   OBSERVED PROPERTIES   #
###########################
@v1.post("/ObservedProperties")
async def create_observed_property(
        observed_property: ObservedProperty,
        pgpool=Depends(get_pool)):
    print(observed_property)
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                    INSERT INTO public.observed_property
                    (name, description, definition, constraint)
                    VALUES($1, $2, $3, $4)
                """,
                observed_property.name,
                observed_property.description,
                observed_property.definition,
                observed_property.constraint
            )
            print(result)
            if result:
                result = {
                    "@iot.selfLink": "http://localhost/v.1./ObservedProperties({})".format(
                        result[0][0]
                    )
                }
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


@v1.get("/ObservedProperties({observed_property_id})")
async def get_observed_property(observed_property_id: str, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                    SELECT * FROM public.observed_property
                    WHERE id::text = $1
                """,
                observed_property_id,
            )
            if result:
                return UJSONResponse(status_code=status.HTTP_200_OK, content=dict(result))
            else:
                return UJSONResponse(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return str(e)


@v1.get("/ObservedProperties")
async def get_observed_properties(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetch(
                "SELECT * FROM public.observed_property"
            )
            if result:
                return UJSONResponse(status_code=status.HTTP_200_OK, content=[dict(r) for r in result])
            else:
                return UJSONResponse(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return str(e)


@v1.put("/ObservedProperties({observed_property_id})")
async def update_observed_properties(observed_property_id: int, observed_property: ObservedProperty,  pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                    UPDATE public.observed_property
                    SET name = $1, description = $2, definition= $3, constraint = $4
                    WHERE id = $5
                    RETURNING id
                """,
                observed_property.name,
                observed_property.description,
                observed_property.definition,
                observed_property.constraint,
                observed_property_id
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


@v1.delete("/ObservedProperties({observed_property_id})")
async def delete_observed_property(observed_property_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                """DELETE FROM public.observed_property
                WHERE id = $1
                RETURNING *""",
                observed_property_id,
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