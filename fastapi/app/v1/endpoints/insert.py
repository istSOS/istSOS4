import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from app.utils.utils import create_entity
from fastapi import Depends
from app.db.db import get_pool

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
        result = await create_entity(main_table, body, pgpool)
        # Return okay
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "code": 200,
                "type": "success",
                "message": result
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
