import sys
sys.path.append('../../../src/sta2rest')

import requests
import urllib.parse
from fastapi import FastAPI, Request
from sta2rest import STA2REST

"""
Example query: http://127.0.0.1:8000/Datastream?$filter=thing_id eq 1 or sensor_id eq 2
"""

#fetching database data from postgrest 
app = FastAPI()

#sending response to client
# print(p)
@app.get("/Datastream")
async def root(request: Request):
    try:
        # Get the original query parameters from the request
        query_params = urllib.parse.unquote_plus(str(request.query_params))
        print("Original query: ", query_params)
        # Convert the STA query to a PostgREST query
        converted_query = STA2REST.convert_query(query_params)
        print("Converted query: ", converted_query)

        # Send the converted query to PostgREST
        r = requests.get('http://localhost:3000/Datastream?' + converted_query)

        # Return the response from PostgREST
        return r.json()
    except Exception as e:
        # send the error message to the client
        return {"error": str(e)}