from fastapi import FastAPI, Request
from app.v1 import api
from app.settings import tables, serverSettings
import os

app = FastAPI(debug=True)

def __handle_root():
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": 
                f"{os.getenv('HOSTNAME')}{os.getenv('SUBPATH')}{os.getenv('VERSION')}" + "/" + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    } 
    return response

@app.get(f"{os.getenv('SUBPATH')}{os.getenv('VERSION')}")
async def read_root():
    return __handle_root()

app.mount(f"{os.getenv('SUBPATH')}{os.getenv('VERSION')}", api.v1)

# API SERVIZI
# app.mount("/admin", api.admin)