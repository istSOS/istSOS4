from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, Text

from .database import SCHEMA_NAME, Base


class HistoricalLocationTravelTime(Base):
    __tablename__ = "HistoricalLocation_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    location_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(
        Integer, ForeignKey(f"{SCHEMA_NAME}.Thing.id"), nullable=False
    )
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )
