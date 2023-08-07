from app.v1.endpoints import read
from app.v1.endpoints import insert
from fastapi import FastAPI

v1 = FastAPI()

# Register the endpoints
v1.include_router(read.v1)
v1.include_router(insert.v1)
