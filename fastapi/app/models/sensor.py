from pydantic import BaseModel, Extra
from .contact import Contact
from datetime import timedelta


class SensorType(BaseModel):
    description: str | None = None
    metadata: str | None = None


class Sensor(BaseModel):
    name: str | None = None
    description: str | None = None
    encoding_type: str | None = None
    sampling_time_resolution: timedelta | None = None
    acquisition_time_resolution: timedelta | None = None
    sensor_type: SensorType | int | None = None
    contact: Contact | int | None = None

    class Config:
        extra = Extra.allow