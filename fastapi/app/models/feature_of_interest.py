from .database import Base, SCHEMA_NAME
from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON, TSTZRANGE
from geoalchemy2 import Geometry
from shapely.wkb import loads as wkb_loads
import json

class FeaturesOfInterest(Base):
    __tablename__ = 'FeaturesOfInterest'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    observations_navigation_link = Column("Observations@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    feature = Column(Geometry(geometry_type='GEOMETRY', srid=4326), nullable=False)
    properties = Column(JSON)
    observation = relationship("Observation", back_populates="featuresofinterest")

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "observations_navigation_link": "Observations@iot.navigationLink",
            "encoding_type": "encodingType",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        if 'feature' in serialized_data and self.feature is not None:
            shapely_geom = wkb_loads(bytes(self.feature.data))
            geojson_dict = json.loads(json.dumps(shapely_geom.__geo_interface__))
            serialized_data['feature'] = geojson_dict
        return serialized_data

    def to_dict_expand(self):
        """Serialize the FeaturesOfInterest model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if 'observation' not in inspect(self).unloaded:
            data['Observations'] = [observation.to_dict_expand() for observation in self.observation]
        return data

    def to_dict(self):
        """Serialize the FeaturesOfInterest model to a dict without expanding relationships."""
        return self._serialize_columns()