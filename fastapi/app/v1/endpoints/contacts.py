from app.db.db import get_pool
from app.models.contact import Contact
from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import UJSONResponse


v1 = APIRouter()

######################
#   CONTACTS TYPES   #
######################
@v1.get("/contact_types/")
async def get_contact_types(pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT enum_range(NULL:: public.contact_type) AS contact_types"
            )
            return UJSONResponse(status_code=status.HTTP_200_OK, content=dict(result))
    except Exception as e:
        return str(e)
################
#   CONTACTS   #
################
@v1.post("/contacts/")
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
    except Exception as e:
        return str(e)


@v1.get("/contacts/{contact_id}")
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
    except Exception as e:
        return str(e)


@v1.get("/contacts/")
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
    except Exception as e:
        return str(e)


@v1.put("/contacts/{contact_id}")
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
    except Exception as e:
        return str(e)


@v1.delete("/contacts/{contact_id}")
async def delete_contact(contact_id: int, pgpool=Depends(get_pool)):
    try:
        async with pgpool.acquire() as conn:
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
    except Exception as e:
        return str(e)