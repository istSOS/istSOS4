from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class SensorTravelTime(Base):
    __tablename__ = "Sensor_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    sensor_metadata = Column("metadata", JSON, nullable=False)
    properties = Column(JSON)
    commit_id = Column(Integer)

    datastream = Column("Datastream")
    commit = Column("Commit")
    system_time_validity = Column("systemTimeValidity", TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )
