from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, Text

from .database import SCHEMA_NAME, Base
from .location_historicallocation import Location_HistoricalLocation


class HistoricalLocation(Base):
    __tablename__ = "HistoricalLocation"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    location_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Thing.id"),
        nullable=False,
    )
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    location = relationship(
        "Location",
        secondary=Location_HistoricalLocation,
        back_populates="historicallocation",
    )
    thing = relationship("Thing", back_populates="historicallocation")
    commit = relationship("Commit", back_populates="historicallocation")
