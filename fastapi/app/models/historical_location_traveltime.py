from .database import Base, SCHEMA_NAME
from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import TIMESTAMP, TSTZRANGE

class HistoricalLocationTravelTime(Base):
    __tablename__ = 'HistoricalLocation_traveltime'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    locations_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), nullable=False)
    location_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), nullable=False)
    system_time_validity = Column(TSTZRANGE)

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "locations_navigation_link": "Locations@iot.navigationLink",
            "thing_navigation_link": "Thing@iot.navigationLink",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if 'time' in serialized_data and self.time is not None:
            serialized_data['time'] = self.time.isoformat()
        return serialized_data

    def to_dict_expand(self):
        """Serialize the HistoricalLocationTravelTime model to a dict, excluding 'system_time_validity'."""
        data = self._serialize_columns()
        data.pop('system_time_validity', None)
        return data