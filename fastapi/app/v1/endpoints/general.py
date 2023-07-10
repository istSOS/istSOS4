from fastapi import APIRouter, Request
from app.sta2rest import sta2rest
import httpx

v1 = APIRouter()

@v1.api_route("/{path_name:path}", methods=["GET"])
async def catch_all(request: Request, path_name: str):
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

            if result['single_result']:
                data = data[0]
                if result['value']:
                    # get the value of the first key
                    data = data[list(data.keys())[0]]  
            elif result['ref']:
                # Get the value of the first key
                key_name = list(data[0].keys())[0]
                rows = data[0][key_name]
                
                data = {
                    "value": []
                }

                table_name = key_name + "s"

                host = request.url.scheme + "://" + request.url.netloc

                for row in rows:
                    data["value"].append({
                        "@iot.selfLink": f"{host}/v1.1/{table_name}({row['id']})"
                    })

            return data
    except Exception as e:
        return {"error": str(e)}
