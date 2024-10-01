from app.db.sqlalchemy_db import SCHEMA_NAME, Base
from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .location_historicallocation import Location_HistoricalLocation
from .thing_location import Thing_Location


class Location(Base):
    __tablename__ = "Location"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Things@iot.navigationLink", Text)
    historicallocation_navigation_link = Column(
        "HistoricalLocations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    location = Column(Geometry, nullable=False)
    properties = Column(JSON)
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    thing = relationship(
        "Thing", secondary=Thing_Location, back_populates="location"
    )
    historicallocation = relationship(
        "HistoricalLocation",
        secondary=Location_HistoricalLocation,
        back_populates="location",
    )
    commit = relationship("Commit", back_populates="location")
