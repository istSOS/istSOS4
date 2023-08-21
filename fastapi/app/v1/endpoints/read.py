import httpx
import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest
from app.utils.utils import PostgRESTError, __create_ref_format, __flatten_expand_entity, __flatten_navigation_links
from app.settings import tables, serverSettings

v1 = APIRouter()


def __handle_root(request: Request):
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": 
                request.url._url + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    } 
    return response

@v1.api_route("/{path_name:path}", methods=["GET"])
async def catch_all_get(request: Request, path_name: str):
    if not path_name:
        # Handle the root path
        return __handle_root(request)

    try:    
        # get full path from request
        full_path = request.url.path
        if request.url.query:
            full_path += "?" + request.url.query

        result = sta2rest.STA2REST.convert_query(full_path)
        
        path = result["url"]

        print("original:\t", full_path)
        print("url:\t\t", path)

        url = "http://postgrest:3000" + path

        async with httpx.AsyncClient() as client:
            r = await client.get(url)
            data = r.json()

            # print r status
            if r.status_code != 200:
                raise PostgRESTError(data["message"])

            if result['single_result']:
                data = __flatten_expand_entity(data)

                # check if the result is an array
                if isinstance(data, list):
                    
                    # check if the array is empty
                    return JSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={
                            "code": 404,
                            "type": "error",
                            "message": "Not Found"
                        }
                    )

                    data = data[0]

                if result['value']:
                    # get the value of the first key
                    data = data[list(data.keys())[0]]  
                elif result['ref']:
                    data = __create_ref_format(data)
                elif "@iot.navigationLink" in data:
                    __flatten_navigation_links(data)

            elif result['ref']:
                data = __create_ref_format(data)
            else:
                data = __flatten_expand_entity(data)
                # check if the result is an array
                if not isinstance(data, list):
                    data = [data] if data else []

                for row in data:
                    __flatten_navigation_links(row)

                data = {
                    "value": data
                }
            return data
    except PostgRESTError as pge:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "code": 404,
                "type": "error",
                "message": str(pge)
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
