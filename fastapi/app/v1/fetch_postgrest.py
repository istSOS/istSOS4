import requests
from fastapi import FastAPI

#fetching database data from postgrest 
app = FastAPI()


#sending response to client
# print(p)
@app.get("/datastream")
async def root():
    r = requests.get('http://localhost:3000/Datastream')
    p=r.json()
    return p
