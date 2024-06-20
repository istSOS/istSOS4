import os

from app.settings import serverSettings, tables
from app.v1 import api

from fastapi import FastAPI, Request

app = FastAPI(debug=True)


def __handle_root():
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": f"{os.getenv('HOSTNAME')}{os.getenv('SUBPATH')}{os.getenv('VERSION')}"
                + "/"
                + table,
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
