import httpx
import traceback
from fastapi import APIRouter, Request
from app.sta2rest import sta2rest

v1 = APIRouter()

tables = ["Datastreams", "FeaturesOfInterest", "HistoricalLocations", "Locations", "Observations", "ObservedProperties", "Sensors", "Things"]
serverSettings = {
    "conformance": [
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data",
    ],
}


def __flatten_navigation_links(row):
    if "@iot.navigationLink" in row:
        # merge all the keys from the navigationLink
        row.update(row["@iot.navigationLink"])
        del row["@iot.navigationLink"]

def __flatten_expand_entity(data):
    # Check if it is an array
    if not isinstance(data, list):
        # throw an error
        raise Exception(data)

    # Check if there is only one key and it is in an ENTITY_MAPPING from the sta2rest module
    if len(data[0].keys()) == 1 and list(data[0].keys())[0] in sta2rest.STA2REST.ENTITY_MAPPING:
        # Get the value of the first key
        key_name = list(data[0].keys())[0]
        data = data[0][key_name]

    return data

@v1.api_route("/{path_name:path}", methods=["GET"])
async def catch_all(request: Request, path_name: str):
    try:
        if not path_name:
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

            if result['single_result']:
                data = __flatten_expand_entity(data)[0]
                if result['value']:
                    # get the value of the first key
                    data = data[list(data.keys())[0]]  
                elif "@iot.navigationLink" in data:
                    __flatten_navigation_links(data)

            elif result['ref']:
                # Get the value of the first key
                key_name = list(data[0].keys())[0]
                rows = data[0][key_name]
                data = {
                    "value": []
                }
                for row in rows:
                    data["value"].append({
                        "@iot.selfLink": row["@iot.selfLink"]
                    })
            else:
                data = __flatten_expand_entity(data)

                for row in data:
                    __flatten_navigation_links(row)

                data = {
                    "value": data
                }

            return data
    except Exception as e:
        # print stack trace
        traceback.print_exc()
        return {"error": str(e)}

