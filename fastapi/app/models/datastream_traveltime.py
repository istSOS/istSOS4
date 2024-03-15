from .database import Base, SCHEMA_NAME
from sqlalchemy import Column, Integer, Text, String, ForeignKey
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import JSON, TIMESTAMP, TSTZRANGE
from geoalchemy2 import Geometry
from shapely.wkb import loads as wkb_loads
import json

class DatastreamTravelTime(Base):
    __tablename__ = 'Datastream_traveltime'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    sensor_navigation_link = Column("Sensor@iot.navigationLink", Text)
    observed_property_navigation_link = Column("ObservedProperty@iot.navigationLink", Text)
    observations_navigation_link = Column("Observations@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    unit_of_measurement = Column("unitOfMeasurement", JSON, nullable=False)
    observation_type = Column("observationType", String(100), nullable=False)
    observed_area = Column("observedArea", Geometry(geometry_type='POLYGON', srid=4326))
    phenomenon_time = Column("phenomenonTime", TSTZRANGE)
    result_time = Column("resultTime", TIMESTAMP)
    properties = Column(JSON)
    thing_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), nullable=False)
    sensor_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Sensor.id'), nullable=False)
    observedproperty_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.ObservedProperty.id'), nullable=False)
    system_time_validity = Column(TSTZRANGE)

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "thing_navigation_link": "Thing@iot.navigationLink",
            "sensor_navigation_link": "Sensor@iot.navigationLink",
            "observed_property_navigation_link": "ObservedProperty@iot.navigationLink",
            "observations_navigation_link": "Observations@iot.navigationLink",
            "unit_of_measurement": "unitOfMeasurement",
            "observation_type": "observationType",
            "observed_area": "observedArea",
            "phenomenon_time": "phenomenonTime",
            "result_time": "resultTime",
        }
        serialized_data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'observedArea' in serialized_data and self.observed_area is not None:
            shapely_geom = wkb_loads(bytes(self.observed_area.data))
            geojson_dict = json.loads(json.dumps(shapely_geom.__geo_interface__))
            serialized_data['observedArea'] = geojson_dict
        if 'phenomenonTime' in serialized_data and self.phenomenon_time is not None:
            serialized_data['phenomenonTime'] = self._format_datetime_range(self.phenomenon_time)
        if 'resultTime' in serialized_data and self.result_time is not None:
            serialized_data['resultTime'] = self._format_datetime_range(self.result_time)
        return serialized_data

    def to_dict_expand(self):
        """Serialize the DatastreamTravelTime model to a dict, excluding 'system_time_validity'."""
        data = self._serialize_columns()
        data.pop('system_time_validity', None)
        return 

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, 'lower', None)
            upper = getattr(range_obj, 'upper', None)
            return {
                "start": lower.isoformat() if lower else None,
                "end": upper.isoformat() if upper else None
            }
        return None