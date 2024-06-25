from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base


class SensorTravelTime(Base):
    __tablename__ = "Sensor_traveltime"

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    sensor_metadata = Column("metadata", JSON, nullable=False)
    properties = Column(JSON)
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (
        PrimaryKeyConstraint(id, system_time_validity),
        {"schema": SCHEMA_NAME},
    )

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations and excluding specific fields if necessary."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "datastream_navigation_link": "Datastreams@iot.navigationLink",
            "encoding_type": "encodingType",
            "sensor_metadata": "metadata",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if (
            "system_time_validity" in serialized_data
            and self.system_time_validity is not None
        ):
            serialized_data["system_time_validity"] = (
                self._format_datetime_range(self.system_time_validity)
            )
        return serialized_data

    def to_dict_expand(self):
        """Serialize the SensorTravelTime model to a dict, excluding 'system_time_validity'."""
        return self._serialize_columns()

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, "lower", None)
            upper = getattr(range_obj, "upper", None)
            return f"{lower.isoformat()}/{upper.isoformat()}"

        return None
