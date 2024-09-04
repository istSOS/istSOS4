from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class FeaturesOfInterestTravelTime(Base):
    __tablename__ = "FeaturesOfInterest_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    feature = Column(
        Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False
    )
    properties = Column(JSON)
    commit_id = Column(Integer)

    observation = Column("Observation")
    commit = Column("Commit")
    system_time_validity = Column("systemTimeValidity", TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )
