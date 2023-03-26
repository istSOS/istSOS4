from app.v1.endpoints import observed_properties as op
from app.v1.endpoints import sensors
from app.v1.endpoints import contacts
from app.v1.endpoints import observations
from fastapi import FastAPI

v1 = FastAPI()

v1.include_router(op.v1)
v1.include_router(sensors.v1)
v1.include_router(contacts.v1)
v1.include_router(observations.v1)