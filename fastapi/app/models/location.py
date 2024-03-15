from .database import Base, SCHEMA_NAME
from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON
from geoalchemy2 import Geometry
from shapely.wkb import loads as wkb_loads
import json

class Location(Base):
    __tablename__ = 'Location'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    things_navigation_link = Column("Things@iot.navigationLink", Text)
    historical_locations_navigation_link = Column("HistoricalLocations@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    location = Column(Geometry(geometry_type='GEOMETRY', srid=4326), nullable=False)
    properties = Column(JSON)
    thing = relationship("Thing", back_populates="location")
    historicallocation = relationship("HistoricalLocation", back_populates="location")
        
    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "things_navigation_link": "Things@iot.navigationLink",
            "historical_locations_navigation_link": "HistoricalLocations@iot.navigationLink",
            "encoding_type": "encodingType",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if 'location' in serialized_data and self.location is not None:
            shapely_geom = wkb_loads(bytes(self.location.data))
            geojson_dict = json.loads(json.dumps(shapely_geom.__geo_interface__))
            serialized_data['location'] = geojson_dict
        return serialized_data

    def to_dict_expand(self):
        """Serialize the Location model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if 'thing' not in inspect(self).unloaded:
            data['Things'] = [thing.to_dict_expand() for thing in self.thing]
        if 'historicallocation' not in inspect(self).unloaded:
            data['Historicallocations'] = [hl.to_dict_expand() for hl in self.historicallocation]
        return data

    def to_dict(self):
        """Serialize the Location model to a dict without expanding relationships."""
        return self._serialize_columns()