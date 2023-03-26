from app.db.db import get_pool
from app.models.observation import Observation
from app.models.query_parameters import QueryParameters
from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import UJSONResponse

v1 = APIRouter()

from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.background import BackgroundTask
import httpx
import json

###################
#   OBSERVATION   #
###################
# from typing import List

@v1.post("/Observation")
async def insert_observation(observation: Observation):
    try:
        url = "http://postgrest:3000/Observation"
        print("GO IT!!!")
        async with httpx.AsyncClient() as client:
            print(observation.dict(exclude_none=True))
            print(dict(observation))
            # result = await client.post(url, data=json.dumps(dict(observation),default=str ))
            result = await client.post(url, data=observation.dict(exclude_none=True))
            print('result', result)
        if result and result.status_code == 201:
            print('CREATED!')
            return UJSONResponse(status_code=status.HTTP_201_CREATED, content='result')
    except Exception as e:
        print('except', e)
        return str(e)

@v1.put("/Observation({id})")
async def insert_observation(id: int, observation: Observation):
    try:
        url = "http://postgrest:3000/Observation"
        print("GO IT!!!")
        print('ID:', id, {'id': f'eq.{id}'})
        async with httpx.AsyncClient() as client:
            # print(observation.dict(exclude_none=True))
            # print(dict(observation))
            # result = await client.post(url, data=json.dumps(dict(observation),default=str ))
            result = await client.put(
                url, 
                data=observation.dict(exclude_none=True), 
                params={'id': f'eq.{id}'}
            )
            print('result', result)
        if result and result.status_code == 204:
            print('UPDATED!')
            return UJSONResponse(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        print('except', e)
        return str(e)


# @v1.get("/contacts/{contact_id}")
# async def get_contact(contact_id: int, pgpool=Depends(get_pool)):
#     try:
#         async with pgpool.acquire() as conn:
#             result = await conn.fetchrow(
#                 f"""
#                     SELECT * FROM public.contact
#                     WHERE id = $1
#                 """,
#                 contact_id,
#             )
#             if result:
#                 return UJSONResponse(status_code=status.HTTP_200_OK, content=dict(result))
#             else:
#                 result = {
#                     "exceptionReport": {
#                         "code": "InvalidParameterValue",
#                         "locator": "id"
#                     }
#                 }
#                 return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
#     except Exception as e:
#         return str(e)


# @v1.get("/contacts/")
# async def get_contacts(pgpool=Depends(get_pool)):
#     try:
#         async with pgpool.acquire() as conn:
#             result = await conn.fetch(
#                 "SELECT * FROM public.contact"
#             )
#             if result:
#                 return UJSONResponse(status_code=status.HTTP_200_OK, content=[dict(r) for r in result])
#             else:
#                 result = {
#                     "exceptionReport": {
#                         "code": "InvalidParameterValue",
#                         "locator": "id"
#                     }
#                 }
#                 return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
#     except Exception as e:
#         return str(e)


# @v1.put("/contacts/{contact_id}")
# async def update_contact(contact_id: int, contact: Contact,  pgpool=Depends(get_pool)):
#     try:
#         async with pgpool.acquire() as conn:
#             result = await conn.fetchrow(
#                 """ UPDATE public.contact
#                 SET type = $1, person = $2, telephone = $3, fax = $4, email = $5, web = $6, address = $7, city = $8, admin_area = $9, postal_code = $10, country = $11
#                 WHERE id = $12
#                 RETURNING id""",
#                 contact.type,
#                 contact.person,
#                 contact.telephone,
#                 contact.fax,
#                 contact.email,
#                 contact.web,
#                 contact.address,
#                 contact.city,
#                 contact.admin_area,
#                 contact.postal_code,
#                 contact.country,
#                 contact_id,
#             )
#             if result:
#                 return Response(status_code=status.HTTP_204_NO_CONTENT)
#             else:
#                 result = {
#                     "exceptionReport": {
#                         "code": "InvalidParameterValue",
#                         "locator": "id"
#                     }
#                 }
#                 return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
#     except Exception as e:
#         return str(e)


# @v1.delete("/contacts/{contact_id}")
# async def delete_contact(contact_id: int, pgpool=Depends(get_pool)):
#     try:
#         async with pgpool.acquire() as conn:
#             result = await conn.fetchrow(
#                 f"""DELETE FROM public.contact
#                 WHERE id = $1
#                 RETURNING id""",
#                 contact_id,
#             )
#             if result:
#                 return Response(status_code=status.HTTP_204_NO_CONTENT)
#             else:
#                 result = {
#                     "exceptionReport": {
#                         "code": "InvalidParameterValue",
#                         "locator": "id"
#                     }
#                 }
#                 return UJSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result)
#     except Exception as e:
#         return str(e)


# client = httpx.AsyncClient(base_url="http://localhost:3000/")


# async def _reverse_proxy(request: Request):
#     url = httpx.URL(path=request.url.path,
#                     query=request.url.query.encode("utf-8"))
#     rp_req = client.build_request(request.method, url,
#                                   headers=request.headers.raw,
#                                   content=await request.body())
#     rp_resp = await client.send(rp_req, stream=True)
#     return StreamingResponse(
#         rp_resp.aiter_raw(),
#         status_code=rp_resp.status_code,
#         headers=rp_resp.headers,
#         background=BackgroundTask(rp_resp.aclose),
#     )

# app.add_route("/titles/{path:path}",
#               _reverse_proxy, ["GET", "POST"])
