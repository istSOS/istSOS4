import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi import status
from app.sta2rest import sta2rest
from fastapi import Depends
from app.db.db import get_pool
import json

v1 = APIRouter()

# Handle DELETE requests


@v1.api_route("/{path_name:path}", methods=["DELETE"])
async def catch_all_delete(request: Request, path_name: str, pgpool=Depends(get_pool)):

    try:
        full_path = request.url.path
        # parse uri
        result = sta2rest.STA2REST.parse_uri(full_path)

        ##############################################
        ##############################################
        # Definisci il percorso del file JSON
        file_json = 'requests.json'

        # Leggi il file JSON e salva il contenuto in una variabile
        try:
            with open(file_json, 'r') as file:
                dati = json.load(file)
        except:
            dati = []
        dati.append({
            "path": full_path,
            "method": "DELETE",
            "body": ""
        })
        # Risalva i dati JSON modificati nello stesso file
        with open(file_json, 'w') as file:
            json.dump(dati, file, indent=4)
        ##############################################
        ##############################################

        # Get main entity
        [name, id] = result["entity"]

        # Get the name and id
        if not name:
            raise Exception("No entity name provided")

        if not id:
            raise Exception("No entity id provided")

        async with pgpool.acquire() as conn:
            # Create delete SQL query
            query = f'DELETE FROM sensorthings."{name}" WHERE id = $1 RETURNING id'
            # Execute query
            id_deleted = await conn.fetchval(query, int(id))

            if id_deleted is None:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "code": 404,
                        "type": "error",
                        "message": "Nothing found."
                    }
                )

        # Return okay
        return Response(status_code=status.HTTP_200_OK)

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
