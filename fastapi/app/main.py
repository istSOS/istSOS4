from fastapi import FastAPI, Request
from app.v1 import api
from app.settings import tables, serverSettings
import os

app = FastAPI(debug=True)

def __handle_root(request: Request):
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": 
                request.url._url + "/" + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    } 
    return response

@app.get(f"{os.getenv('SUBPATH')}{os.getenv('VERSION')}")
async def read_root(request: Request):
    return __handle_root(request)

app.mount(f"{os.getenv('SUBPATH')}{os.getenv('VERSION')}", api.v1)

# API SERVIZI
# app.mount("/admin", api.admin)