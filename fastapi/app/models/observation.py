from pydantic import BaseModel, Extra, Json
from datetime import timedelta, datetime
#from psycopg2.extras import DateTimeTZRange, tstzrange

# class TSTZRANGE(BaseModel):
    

class Observation(BaseModel):
    id: int | None = None
    phenomenonTime: datetime
    resultTime: datetime
    result: float
    resultQuality: str | None = None
    validTime: str | None = None
    parameters: Json | None = None
    datastream_id: int
    feature_of_interest_id: int

class put_Observation(BaseModel):
    id: int | None = None
    phenomenonTime: datetime | None = None
    resultTime: datetime | None = None
    result: float | None = None
    resultQuality: str | None = None
    validTime: str | None = None
    parameters: Json | None = None
    datastream_id: int | None = None
    feature_of_interest_id: int | None = None