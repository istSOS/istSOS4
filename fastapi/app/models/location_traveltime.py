from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class LocationTravelTime(Base):
    __tablename__ = "Location_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Things@iot.navigationLink", Text)
    historicallocation_navigation_link = Column(
        "HistoricalLocations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    location = Column(
        Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False
    )
    properties = Column(JSON)
    commit_id = Column(Integer)

    thing = Column("Thing")
    historicallocation = Column("HistoricalLocation")
    commit = Column("Commit")
    system_time_validity = Column("systemTimeValidity", TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )
