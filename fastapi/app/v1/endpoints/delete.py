import traceback
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from app.utils.utils import PostgRESTError

v1 = APIRouter()

# Handle DELETE requests
@v1.api_route("/{path_name:path}", methods=["DELETE"])
async def catch_all_delete(request: Request, path_name: str):

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

        async with httpx.AsyncClient() as client:   
            url = "http://postgrest:3000/" + name + "?id=eq." + id

            # post to postgrest
            r = await client.delete(url)


            if r.status_code != 204:
                result = r.json()
                raise PostgRESTError(result["message"])

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
