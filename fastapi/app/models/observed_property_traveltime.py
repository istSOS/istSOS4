from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import JSON, TSTZRANGE
from .database import Base, SCHEMA_NAME

class ObservedPropertyTravelTime(Base):
    __tablename__ = 'ObservedProperty_traveltime'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    datastreams_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    definition = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    properties = Column(JSON)
    system_time_validity = Column(TSTZRANGE)

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "datastreams_navigation_link": "Datastreams@iot.navigationLink",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        return serialized_data

    def to_dict_expand(self):
        """Serialize the ObservedPropertyTravelTime model to a dict, excluding 'system_time_validity'."""
        data = self._serialize_columns()
        data.pop('system_time_validity', None)
        return data
