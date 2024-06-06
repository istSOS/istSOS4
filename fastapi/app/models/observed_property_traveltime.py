from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE

class ObservedPropertyTravelTime(Base):
    __tablename__ = 'ObservedProperty_traveltime'

    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    definition = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    properties = Column(JSON)
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (PrimaryKeyConstraint(id, system_time_validity), {'schema': SCHEMA_NAME })

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "datastream_navigation_link": "Datastreams@iot.navigationLink",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if 'system_time_validity' in serialized_data and self.system_time_validity is not None:
            serialized_data['system_time_validity'] = self._format_datetime_range(self.system_time_validity)
        return serialized_data

    def to_dict_expand(self):
        """Serialize the ObservedPropertyTravelTime model to a dict, excluding 'system_time_validity'."""
        return self._serialize_columns()

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, 'lower', None)
            upper = getattr(range_obj, 'upper', None)
            return f"{lower.isoformat()}/{upper.isoformat()}"

        return None
