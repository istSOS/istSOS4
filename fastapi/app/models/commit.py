from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class Commit(Base):
    __tablename__ = "Commit"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    location_navigation_link = Column("Location@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    historicallocation_navigation_link = Column(
        "HistoricalLocation@iot.navigationLink", Text
    )
    observedproperty_navigation_link = Column(
        "ObservedProperty@iot.navigationLink", Text
    )
    sensor_navigation_link = Column("Sensor@iot.navigationLink", Text)
    datastream_navigation_link = Column("Datastream@iot.navigationLink", Text)
    featuresofinterest_navigation_link = Column(
        "FeatureOfInterest@iot.navigationLink", Text
    )
    observation_navigation_link = Column(
        "Observation@iot.navigationLink", Text
    )
    author = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100))
    message = Column(String(255), nullable=False)
    date = Column(TIMESTAMP, server_default="now()")
    location = relationship("Location", back_populates="commit")
    thing = relationship("Thing", back_populates="commit")
    historicallocation = relationship(
        "HistoricalLocation", back_populates="commit"
    )
    observedproperty = relationship(
        "ObservedProperty", back_populates="commit"
    )
    sensor = relationship("Sensor", back_populates="commit")
    datastream = relationship("Datastream", back_populates="commit")
    featuresofinterest = relationship(
        "FeaturesOfInterest", back_populates="commit"
    )
    observation = relationship("Observation", back_populates="commit")
