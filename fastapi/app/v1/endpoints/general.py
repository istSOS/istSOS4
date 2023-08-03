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
    if row and "@iot.navigationLink" in row:
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
    
def get_result_type_and_column(input_string):
    try:
        value = eval(str(input_string))
    except (SyntaxError, NameError):
        result_type = 0
        column_name = "resultString"
    else:
        if isinstance(value, int):
            result_type = 1
            column_name = "resultInteger"
        elif isinstance(value, float):
            result_type = 2
            column_name = "resultDouble"
        elif isinstance(value, dict):
            result_type = 4
            column_name = "resultJSON"
        else:
            result_type = None
            column_name = None

    if input_string == "true" or input_string == "false":
        result_type = 3
        column_name = "resultBoolean"

    if result_type is not None:
        return result_type, column_name
    else:
        raise Exception("Cannot cast result to a valid type")
    
def flatten_entity_body(entity, body, name = None):

    # check if entity is an array
    if isinstance(entity, list):
        # loop trought all the values
        for item in entity:
            # create the entity
            flatten_entity_body(item, body, name)
        return body

    for key in list(entity):
        if isinstance(key, str) and key in sta2rest.STA2REST.ENTITY_MAPPING:
            converted_key = sta2rest.STA2REST.convert_entity(key)
            body[converted_key] = entity[key]
            
            flatten_entity_body(entity[key], body, converted_key)
            
            if name:
                if isinstance(body[name], list):
                    for item in body[name]:
                        item[converted_key] = {
                            "@iot.id": None
                        }
                else:
                    body[name][converted_key] = {
                        "@iot.id": None
                    }

    return body

def format_entity_body(entity_body): 
    # Check if entity_body is an array
    if isinstance(entity_body, list):
        # Loop trought all the values
        for i in range(len(entity_body)):
            # Create the entity
            entity_body[i] = format_entity_body(entity_body[i])
        return entity_body

    formatted_body = {}
    # Loop trought all the keys in the body
    for key in entity_body:
        if isinstance(key, str) and key in sta2rest.STA2REST.ENTITY_MAPPING:
            if "@iot.id" in entity_body[key]:
                new_key = sta2rest.STA2REST.convert_to_database_id(key)
                formatted_body[new_key] = entity_body[key]["@iot.id"]
        elif key == "result":
            value = entity_body["result"]
            result_type, column_name = get_result_type_and_column(value)
            formatted_body[column_name] = value
            formatted_body["resultType"] = result_type
        else:
            formatted_body[key] = entity_body[key]

    return formatted_body

async def create_entity(entity_name, body):

    body[entity_name] = {}
    # Loop trough all keys in the body and if they are not an entity create a main entity
    for key in list(body):
        if isinstance(key, str) and key not in sta2rest.STA2REST.ENTITY_MAPPING:
            body[entity_name][key] = body[key]
            del body[key]

    body = flatten_entity_body(body, body)

    # Creation order
    created_ids = {}
    creation_order = ["Thing", "Location", "Sensor", "ObservedProperty", "FeaturesOfInterest", "Datastream", "Observation"]
    for entity_name in creation_order:
        if entity_name in body:
            formatted_body = format_entity_body(body[entity_name])

            url = "http://postgrest:3000/" + entity_name

            print("CREATING ENTITY", entity_name, formatted_body)
            '''
            async with httpx.AsyncClient() as client:   
                # post to postgrest
                r = await client.post(url, json=formatted_body, headers={"Prefer": "return=representation"})

                # get response
                result = r.json()

                # print r status
                if r.status_code != 201:
                    raise PostgRESTError(result["message"])
                
                return result'''

    return None


# Handle POST requests
@v1.api_route("/{path_name:path}", methods=["POST"])
async def catch_all_post(request: Request, path_name: str):
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
        result = await create_entity(main_table, body)
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
