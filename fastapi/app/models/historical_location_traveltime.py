from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, Text

from .database import SCHEMA_NAME, Base


class HistoricalLocationTravelTime(Base):
    __tablename__ = "HistoricalLocation_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    location_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(
        Integer, ForeignKey(f"{SCHEMA_NAME}.Thing.id"), nullable=False
    )
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "location_navigation_link": "Locations@iot.navigationLink",
            "thing_navigation_link": "Thing@iot.navigationLink",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if "time" in serialized_data and self.time is not None:
            serialized_data["time"] = self.time.isoformat()
        if (
            "system_time_validity" in serialized_data
            and self.system_time_validity is not None
        ):
            serialized_data["system_time_validity"] = (
                self._format_datetime_range(self.system_time_validity)
            )
        return serialized_data

    def to_dict_expand(self):
        """Serialize the HistoricalLocationTravelTime model to a dict, excluding 'system_time_validity'."""
        return self._serialize_columns()

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, "lower", None)
            upper = getattr(range_obj, "upper", None)
            return f"{lower.isoformat()}/{upper.isoformat()}"
        return None
