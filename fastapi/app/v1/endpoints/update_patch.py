import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from fastapi import Depends
from app.db.db import get_pool
from app.utils.utils import format_entity_body, prepare_entity_body_for_insert

v1 = APIRouter()

# Handle UPDATE requests
@v1.api_route("/{path_name:path}", methods=["PATCH"])
async def catch_all_update(request: Request, path_name: str, pgpool=Depends(get_pool)):
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

        # Get main entity
        [name, id] = result["entity"]
        
        body = await request.json()
        
        print("BODY PATCH", body)

        # Get the name and id
        if not name:
            raise Exception("No entity name provided")
    
        if not id:
            raise Exception("No entity id provided")

        # Check that the column names (key) contains only alphanumeric characters and underscores
        for key in body.keys():
            if not key.isalnum():
                raise Exception(f"Invalid column name: {key}")
            
        body = format_entity_body(body)
        prepare_entity_body_for_insert(body, {})

        async with pgpool.acquire() as conn:
            if not body:  # Check if body is empty
                query = f'UPDATE sensorthings."{name}" SET id = id WHERE id = ${len(body.keys()) + 1} RETURNING ID;'
            else:
                query = f'UPDATE sensorthings."{name}" SET ' + ', '.join([f'"{key}" = ${i+1}' for i, key in enumerate(body.keys())]) + f' WHERE id = ${len(body.keys()) + 1} RETURNING ID;'
        
            print(query, body.values(), id)

            # Execute query
            id_patch = await conn.fetchval(query, *body.values(), int(id))

            if id_patch is None:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "code": 404,
                        "type": "error",
                        "message": "No entity found for path."
                    }
                )
            
            # Return okay
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "code": 200,
                    "type": "success"
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
