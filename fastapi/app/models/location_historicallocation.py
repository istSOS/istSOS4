from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Table, Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer

Location_HistoricalLocation = Table('Location_HistoricalLocation', Base.metadata,
    Column("location_id", Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), primary_key=True),
    Column("historical_location_id", Integer, ForeignKey(f'{SCHEMA_NAME}.HistoricalLocation.id'), primary_key=True)
)