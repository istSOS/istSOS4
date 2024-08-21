import traceback

from app import DEBUG
from app.db.db import get_pool
from app.sta2rest import sta2rest
from fastapi.responses import JSONResponse, Response

from fastapi import APIRouter, Depends, Request, status

v1 = APIRouter()

try:
    DEBUG = DEBUG
    if DEBUG:
        from app.utils.utils import response2jsonfile
except:
    DEBUG = 0

# Handle DELETE requests


@v1.api_route("/{path_name:path}", methods=["DELETE"])
async def catch_all_delete(
    request: Request, path_name: str, pgpool=Depends(get_pool)
):
    """
    Delete endpoint for catching all DELETE requests.

    Args:
        request (Request): The incoming request object.
        path_name (str): The path name extracted from the URL.
        pgpool: The connection pool to the database.

    Returns:
        Response: The response object indicating the status of the delete operation.

    Raises:
        Exception: If no entity name or id is provided.

    """
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

        async with pgpool.acquire() as conn:
            # Create delete SQL query
            query = (
                f'DELETE FROM sensorthings."{name}" WHERE id = $1 RETURNING id'
            )
            # Execute query
            id_deleted = await conn.fetchval(query, int(id))

            if id_deleted is None:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "code": 404,
                        "type": "error",
                        "message": "Nothing found.",
                    },
                )
        if DEBUG:
            response2jsonfile(request, "", "requests.json")
        # Return okay
        return Response(status_code=status.HTTP_200_OK)

    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"code": 400, "type": "error", "message": str(e)},
        )
