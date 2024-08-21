from app import HOSTNAME, SUBPATH, VERSION
from app.settings import serverSettings, tables
from app.v1 import api

from fastapi import FastAPI

app = FastAPI(debug=True)


def __handle_root():
    # Handle the root path
    value = []
    # append the domain to the path for each table
    for table in tables:
        value.append(
            {
                "name": table,
                "url": f"{HOSTNAME}{SUBPATH}{VERSION}" + "/" + table,
            }
        )

    response = {
        "value": value,
        "serverSettings": serverSettings,
    }
    return response


@app.get(f"{SUBPATH}{VERSION}")
async def read_root():
    return __handle_root()


app.mount(f"{SUBPATH}{VERSION}", api.v1)
