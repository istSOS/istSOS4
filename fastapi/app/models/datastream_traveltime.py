from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class DatastreamTravelTime(Base):
    __tablename__ = "Datastream_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    sensor_navigation_link = Column("Sensor@iot.navigationLink", Text)
    observedproperty_navigation_link = Column(
        "ObservedProperty@iot.navigationLink", Text
    )
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    unit_of_measurement = Column("unitOfMeasurement", JSON, nullable=False)
    observation_type = Column("observationType", String(100), nullable=False)
    observed_area = Column(
        "observedArea", Geometry(geometry_type="POLYGON", srid=4326)
    )
    phenomenon_time = Column("phenomenonTime", TSTZRANGE)
    result_time = Column("resultTime", TSTZRANGE)
    properties = Column(JSON)
    thing_id = Column(
        Integer, ForeignKey(f"{SCHEMA_NAME}.Thing.id"), nullable=False
    )
    sensor_id = Column(
        Integer, ForeignKey(f"{SCHEMA_NAME}.Sensor.id"), nullable=False
    )
    observedproperty_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.ObservedProperty.id"),
        nullable=False,
    )
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )
