from app.db.sqlalchemy_db import SCHEMA_NAME, Base
from sqlalchemy.sql.schema import Column, ForeignKey, Table
from sqlalchemy.sql.sqltypes import Integer

Thing_Location = Table(
    "Thing_Location",
    Base.metadata,
    Column(
        "thing_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Thing.id"),
        primary_key=True,
    ),
    Column(
        "location_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Location.id"),
        primary_key=True,
    ),
    schema=SCHEMA_NAME,
)
