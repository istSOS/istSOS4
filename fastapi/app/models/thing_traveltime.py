from sqlalchemy import Column, Integer, Text, String, ForeignKey
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import JSON, TSTZRANGE
from .database import Base, SCHEMA_NAME

class ThingTravelTime(Base):
    __tablename__ = 'Thing_traveltime'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    locations_navigation_link = Column("Locations@iot.navigationLink", Text)
    historical_locations_navigation_link = Column("HistoricalLocations@iot.navigationLink", Text)
    datastreams_locations_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    properties = Column(JSON)
    location_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), nullable=False)
    system_time_validity = Column(TSTZRANGE)

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "locations_navigation_link": "Locations@iot.navigationLink",
            "historical_locations_navigation_link": "HistoricalLocations@iot.navigationLink",
            "datastreams_locations_navigation_link": "Datastreams@iot.navigationLink",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        return serialized_data

    def to_dict_expand(self):
        """Serialize the ThingTravelTime model to a dict, excluding 'system_time_validity'."""
        data = self._serialize_columns()
        data.pop('system_time_validity', None)
        return data