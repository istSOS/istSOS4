from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Table, Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer

Thing_Location = Table('Thing_Location', Base.metadata,
    Column("thing_id", Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), primary_key=True),
    Column("location_id", Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), primary_key=True)
)
