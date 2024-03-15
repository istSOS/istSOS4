from .database import Base, SCHEMA_NAME
from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import JSON, TSTZRANGE
from geoalchemy2 import Geometry
from shapely.wkb import loads as wkb_loads
import json

class FeaturesOfInterestTravelTime(Base):
    __tablename__ = 'FeaturesOfInterest_traveltime'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    observations_navigation_link = Column("Observations@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    feature = Column(Geometry(geometry_type='GEOMETRY', srid=4326), nullable=False)
    properties = Column(JSON)
    system_time_validity = Column(TSTZRANGE)
    
    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "observations_navigation_link": "Observations@iot.navigationLink",
            "encoding_type": "encodingType",
        }
        serialized_data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'feature' in serialized_data and self.feature is not None:
            shapely_geom = wkb_loads(bytes(self.feature.data))
            geojson_dict = json.loads(json.dumps(shapely_geom.__geo_interface__))
            serialized_data['feature'] = geojson_dict
        return serialized_data

    def to_dict_expand(self):
        """Serialize the FeaturesOfInterestTravelTime model to a dict, excluding 'system_time_validity'."""
        data = self._serialize_columns()
        data.pop('system_time_validity', None)
        return data