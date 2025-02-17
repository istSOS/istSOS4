from app import AUTHORIZATION
from app.v1.endpoints.create import bulk_observation, data_array_observation
from app.v1.endpoints.create import datastream as create_datastream
from app.v1.endpoints.create import (
    feature_of_interest as create_feature_of_interest,
)
from app.v1.endpoints.create import (
    historical_location as create_historical_location,
)
from app.v1.endpoints.create import location as create_location
from app.v1.endpoints.create import login
from app.v1.endpoints.create import observation as create_observation
from app.v1.endpoints.create import (
    observed_property as create_observed_property,
)
from app.v1.endpoints.create import policy as create_policy
from app.v1.endpoints.create import sensor as create_sensor
from app.v1.endpoints.create import thing as create_thing
from app.v1.endpoints.create import user as create_user
from app.v1.endpoints.delete import datastream as delete_datastream
from app.v1.endpoints.delete import (
    feature_of_interest as delete_feature_of_interest,
)
from app.v1.endpoints.delete import (
    historical_location as delete_historical_location,
)
from app.v1.endpoints.delete import location as delete_location
from app.v1.endpoints.delete import observation as delete_observation
from app.v1.endpoints.delete import (
    observed_property as delete_observed_property,
)
from app.v1.endpoints.delete import policy as delete_policy
from app.v1.endpoints.delete import sensor as delete_sensor
from app.v1.endpoints.delete import thing as delete_thing
from app.v1.endpoints.delete import user as delete_user
from app.v1.endpoints.read import datastream as read_datastream
from app.v1.endpoints.read import (
    feature_of_interest as read_feature_of_interest,
)
from app.v1.endpoints.read import (
    historical_location as read_historical_location,
)
from app.v1.endpoints.read import location as read_location
from app.v1.endpoints.read import observation as read_observation
from app.v1.endpoints.read import observed_property as read_observed_property
from app.v1.endpoints.read import policy as read_policy
from app.v1.endpoints.read import read
from app.v1.endpoints.read import sensor as read_sensor
from app.v1.endpoints.read import thing as read_thing
from app.v1.endpoints.read import user as read_user
from app.v1.endpoints.update import datastream as update_datastream
from app.v1.endpoints.update import (
    feature_of_interest as update_feature_of_interest,
)
from app.v1.endpoints.update import (
    historical_location as update_historical_location,
)
from app.v1.endpoints.update import location as update_location
from app.v1.endpoints.update import observation as update_observation
from app.v1.endpoints.update import (
    observed_property as update_observed_property,
)
from app.v1.endpoints.update import policy as update_policy
from app.v1.endpoints.update import sensor as update_sensor
from app.v1.endpoints.update import thing as update_thing
from fastapi import FastAPI

if AUTHORIZATION:
    tags_metadata = [
        {
            "name": "Users",
            "description": "Users of the SensorThings API.",
        },
        {
            "name": "Policies",
            "description": "Policies for the SensorThings API.",
        },
    ]
else:
    tags_metadata = []

tags_metadata += [
    {
        "name": "Catch All",
        "description": "Read operations for SensorThings API.",
    },
    {
        "name": "Locations",
        "description": "Current (and previous) location details for each thing, generally lat/long and elevation.",
    },
    {
        "name": "Things",
        "description": "Real-world sensors that can be integrated into communication network.",
    },
    {
        "name": "HistoricalLocations",
        "description": "A location where a thing has been at a given point in time.",
    },
    {
        "name": "Sensors",
        "description": "Dictionary of the instrument types being used to observe properties.",
    },
    {
        "name": "ObservedProperties",
        "description": "Dictionary of properties being observed. These can be directly measured or calculated.",
    },
    {
        "name": "Datastreams",
        "description": "The properties observed by a thing, and the type of sensor making the observations.",
    },
    {
        "name": "FeaturesOfInterest",
        "description": "A feature about which observations are made.",
    },
    {
        "name": "Observations",
        "description": "Individual measurements recorded at a given point in time.",
    },
]

v1 = FastAPI(
    title="OGC SensorThings API",
    description="A SensorThings API implementation in Python using FastAPI.",
    version="1.1",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
)

# Register the user endpoint
if AUTHORIZATION:
    v1.include_router(login.v1)
    v1.include_router(read_user.v1)
    v1.include_router(create_user.v1)
    v1.include_router(delete_user.v1)
    v1.include_router(read_policy.v1)
    v1.include_router(create_policy.v1)
    v1.include_router(update_policy.v1)
    v1.include_router(delete_policy.v1)

# Register the read endpoints
v1.include_router(read_location.v1)
v1.include_router(read_thing.v1)
v1.include_router(read_historical_location.v1)
v1.include_router(read_sensor.v1)
v1.include_router(read_observed_property.v1)
v1.include_router(read_datastream.v1)
v1.include_router(read_feature_of_interest.v1)
v1.include_router(read_observation.v1)
v1.include_router(read.v1)

# Register the create endpoints
v1.include_router(bulk_observation.v1)
v1.include_router(data_array_observation.v1)
v1.include_router(create_location.v1)
v1.include_router(create_thing.v1)
v1.include_router(create_historical_location.v1)
v1.include_router(create_sensor.v1)
v1.include_router(create_observed_property.v1)
v1.include_router(create_datastream.v1)
v1.include_router(create_feature_of_interest.v1)
v1.include_router(create_observation.v1)

# Register the update endpoints
v1.include_router(update_location.v1)
v1.include_router(update_thing.v1)
v1.include_router(update_historical_location.v1)
v1.include_router(update_sensor.v1)
v1.include_router(update_observed_property.v1)
v1.include_router(update_datastream.v1)
v1.include_router(update_feature_of_interest.v1)
v1.include_router(update_observation.v1)

# Register the delete endpoints
v1.include_router(delete_location.v1)
v1.include_router(delete_thing.v1)
v1.include_router(delete_historical_location.v1)
v1.include_router(delete_observed_property.v1)
v1.include_router(delete_sensor.v1)
v1.include_router(delete_datastream.v1)
v1.include_router(delete_feature_of_interest.v1)
v1.include_router(delete_observation.v1)
