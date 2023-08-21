import traceback
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from fastapi import Depends
from app.db.db import get_pool

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
        
        # Get the name and id
        if not name:
            raise Exception("No entity name provided")
    
        if not id:
            raise Exception("No entity id provided")
        

        body = await request.json()

        # Check that the column names (key) contains only alphanumeric characters and underscores
        for key in body.keys():
            if not key.isalnum():
                raise Exception(f"Invalid column name: {key}")

        async with pgpool.acquire() as conn:
            # Generate the Update SQL query from the body
            query = f'UPDATE sensorthings."{name}" SET ' + ', '.join([f'"{key}" = ${i+1}' for i, key in enumerate(body.keys())]) + f' WHERE id = ${len(body.keys()) + 1};'
        
            print(query, body.values(), id)

            # Execute query
            await conn.execute(query, *body.values(), int(id))

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
