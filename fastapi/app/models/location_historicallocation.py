from sqlalchemy.sql.schema import Column, ForeignKey, Table
from sqlalchemy.sql.sqltypes import Integer

from .database import SCHEMA_NAME, Base

Location_HistoricalLocation = Table(
    "Location_HistoricalLocation",
    Base.metadata,
    Column(
        "location_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Location.id"),
        primary_key=True,
    ),
    Column(
        "historicallocation_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.HistoricalLocation.id"),
        primary_key=True,
    ),
    schema=SCHEMA_NAME,
)
