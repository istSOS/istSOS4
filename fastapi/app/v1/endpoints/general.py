import httpx
import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi import status
from app.sta2rest import sta2rest

v1 = APIRouter()

tables = ["Datastreams", "FeaturesOfInterest", "HistoricalLocations", "Locations", "Observations", "ObservedProperties", "Sensors", "Things"]
serverSettings = {
    "conformance": [
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data",
    ],
}

class PostgRESTError(Exception):
    pass


def __flatten_navigation_links(row):
    if "@iot.navigationLink" in row:
        # merge all the keys from the navigationLink
        row.update(row["@iot.navigationLink"])
        del row["@iot.navigationLink"]

        # check if skip@iot.navigationLink is present and remove it
        if "skip@iot.navigationLink" in row:
            del row["skip@iot.navigationLink"]

def __flatten_expand_entity(data):
    # Check if it is an array
    if not isinstance(data, list):
        # throw an error
        raise PostgRESTError(data)

    # check if data is empty
    if not data:
        return data
    
    # Check if there is only one key and it is in an ENTITY_MAPPING from the sta2rest module
    if len(data[0].keys()) == 1 and list(data[0].keys())[0] in sta2rest.STA2REST.ENTITY_MAPPING:
        # Get the value of the first key
        key_name = list(data[0].keys())[0]
        data = data[0][key_name]

    return data

def __create_ref_format(data):

    rows = [data]

    # Check if it is an array
    if isinstance(data, list):
        key_name = list(data[0].keys())[0]
        # Check if the key is in an ENTITY_MAPPING from the sta2rest module
        if key_name in sta2rest.STA2REST.ENTITY_MAPPING:
            rows = data[0][key_name]
            if not isinstance(rows, list):
                rows = [rows]
        else:
            rows = data

    data = {
        "value": []
    }

    for row in rows:
        data["value"].append({
            "@iot.selfLink": row["@iot.selfLink"]
        })
    
    return data

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
                    data = [data]

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

# Handle POST requests
@v1.api_route("/{path_name:path}", methods=["POST"])
async def catch_all_post(request: Request, path_name: str):
    try:
        full_path = request.url.path

        # parse uri
        result = sta2rest.STA2REST.parse_uri(full_path)


        print("original:\t", full_path)
        print("result:\t\t", result)

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
