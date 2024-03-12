from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, ForeignKey, Text, Integer, Float, String, Boolean, JSON, TIMESTAMP, inspect
from geoalchemy2 import Geometry

SCHEMA_NAME = 'sensorthings'

Base = declarative_base()

class Location(Base):
    __tablename__ = 'Location'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    thingsNavigationLink = Column("Things@iot.navigationLink", Text)
    historicalLocationsNavigationLink = Column("HistoricalLocations@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    encodingType = Column(String(100), nullable=False)
    location = Column(Geometry(geometry_type='GEOMETRY', srid=4326), nullable=False)
    properties = Column(JSON)
    thing = relationship("Thing", back_populates="location")
    historicallocation = relationship("HistoricalLocation", back_populates="location")
        
    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "thingsNavigationLink": "Things@iot.navigationLink",
            "historicalLocationsNavigationLink": "HistoricalLocations@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'location' in data and data['location'] is not None:
            data['location'] = str(data['location'])
        if 'thing' not in inspect(self).unloaded:
            things = getattr(self, 'thing', [])
            data['Things'] = [thing.to_dict_expand() for thing in things]
        if 'historicallocation' not in inspect(self).unloaded:
            historicallocations = getattr(self, 'historicallocation', [])
            data['Historicallocations'] = [historicallocation.to_dict_expand() for historicallocation in historicallocations]
        return data

    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "thingsNavigationLink": "Things@iot.navigationLink",
            "historicalLocationsNavigationLink": "HistoricalLocations@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'location' in data and data['location'] is not None:
            data['location'] = str(data['location'])
        return data

class Thing(Base):
    __tablename__ = 'Thing'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    locationsNavigationLink = Column("Locations@iot.navigationLink", Text)
    historicalLocationsNavigationLink = Column("HistoricalLocations@iot.navigationLink", Text)
    datastreamsLocationsNavigationLink = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    properties = Column(JSON)
    location_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), nullable=False)
    location = relationship("Location", back_populates="thing")
    datastream = relationship("Datastream", back_populates="thing")
    historicallocation = relationship("HistoricalLocation", back_populates="thing")

    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "locationsNavigationLink": "Locations@iot.navigationLink",
            "historicalLocationsNavigationLink": "HistoricalLocations@iot.navigationLink",
            "datastreamsLocationsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'datastream' not in inspect(self).unloaded:
            datastreams = getattr(self, 'datastream', [])
            data['Datastreams'] = [datastream.to_dict_expand() for datastream in datastreams]
        if 'historicallocation' not in inspect(self).unloaded:
            historicallocations = getattr(self, 'historicallocation', [])
            data['HistoricalLocations'] = [historicallocation.to_dict_expand() for historicallocation in historicallocations]
        for relationships in ['location']:
            if relationships not in inspect(self).unloaded:
                related_obj = getattr(self, relationships, None)
                if related_obj is not None:
                    relationship_key = relationships[0].upper() + relationships[1:]
                    data[relationship_key] = related_obj.to_dict_expand()
        return data

    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "locationsNavigationLink": "Locations@iot.navigationLink",
            "historicalLocationsNavigationLink": "HistoricalLocations@iot.navigationLink",
            "datastreamsLocationsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        return data

class HistoricalLocation(Base):
    __tablename__ = 'HistoricalLocation'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    locationsNavigationLink = Column("Locations@iot.navigationLink", Text)
    thingNavigationLink = Column("Thing@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), nullable=False)
    thing = relationship("Thing", back_populates="historicallocation")
    location_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), nullable=False)
    location = relationship("Location", back_populates="historicallocation")

    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "locationsNavigationLink": "Locations@iot.navigationLink",
            "thingNavigationLink": "Thing@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        for relationships in ['thing', 'location']:
            if relationships not in inspect(self).unloaded:
                related_obj = getattr(self, relationships, None)
                if related_obj is not None:
                    relationship_key = relationships[0].upper() + relationships[1:]
                    data[relationship_key] = related_obj.to_dict_expand()
        if 'time' in data and data['time'] is not None:
            data['time'] = data['time'].isoformat()
        return data
        
    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "locationsNavigationLink": "Locations@iot.navigationLink",
            "thingNavigationLink": "Thing@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'time' in data and data['time'] is not None:
            data['time'] = data['time'].isoformat()
        return data

class ObservedProperty(Base):
    __tablename__ = 'ObservedProperty'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    datastreamsNavigationLink = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    definition = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    properties = Column(JSON)
    datastream = relationship("Datastream", back_populates="observedproperty")

    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "datastreamsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {rename_map.get(attr.key, attr.key): getattr(self, attr.key)
                for attr in self.__class__.__mapper__.column_attrs
                if attr.key not in inspect(self).unloaded
        }
        if 'datastream' not in inspect(self).unloaded:
            datastreams = getattr(self, 'datastream', [])
            data['Datastreams'] = [datastream.to_dict_expand() for datastream in datastreams]
        return data

    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "datastreamsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {rename_map.get(attr.key, attr.key): getattr(self, attr.key)
                for attr in self.__class__.__mapper__.column_attrs
                if attr.key not in inspect(self).unloaded
        }
        return data

class Sensor(Base):
    __tablename__ = 'Sensor'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    datastreamsNavigationLink = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255), nullable=False)
    encodingType = Column(String(100), nullable=False)
    sensor_metadata = Column('metadata', JSON, nullable=False) 
    properties = Column(JSON)
    datastream = relationship("Datastream", back_populates="sensor")

    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "datastreamsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'datastream' not in inspect(self).unloaded:
            datastreams = getattr(self, 'datastream', [])
            data['Datastreams'] = [datastream.to_dict_expand() for datastream in datastreams]
        return data

    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "datastreamsNavigationLink": "Datastreams@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        return data
    
class Datastream(Base):
    __tablename__ = 'Datastream'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    thingNavigationLink = Column("Thing@iot.navigationLink", Text)
    sensorNavigationLink = Column("Sensor@iot.navigationLink", Text)
    observedPropertyNavigationLink = Column("ObservedProperty@iot.navigationLink", Text)
    observationsNavigationLink = Column("Observations@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    unitOfMeasurement = Column(JSON, nullable=False)
    observationType = Column(String(100), nullable=False)
    observedArea = Column(Geometry(geometry_type='POLYGON', srid=4326))
    phenomenonTime = Column(TIMESTAMP)
    resultTime = Column(TIMESTAMP)
    properties = Column(JSON)
    thing_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), nullable=False)
    thing = relationship("Thing", back_populates="datastream")
    sensor_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Sensor.id'), nullable=False)
    sensor = relationship("Sensor", back_populates="datastream")
    observedproperty_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.ObservedProperty.id'), nullable=False)
    observedproperty = relationship("ObservedProperty", back_populates="datastream")
    observation = relationship("Observation", back_populates="datastream")

    def to_dict_expand(self):
        rename_map = {
            'id': '@iot.id',
            'selfLink': '@iot.selfLink',
            'thingNavigationLink': 'Thing@iot.navigationLink',
            'sensorNavigationLink': 'Sensor@iot.navigationLink',
            'observedPropertyNavigationLink': 'ObservedProperty@iot.navigationLink',
            'observationsNavigationLink': 'Observations@iot.navigationLink',
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        for relationships in ['thing', 'sensor', 'observedproperty']:
            if relationships not in inspect(self).unloaded:
                related_obj = getattr(self, relationships, None)
                if related_obj is not None:
                    relationship_key = relationships[0].upper() + relationships[1:]
                    data[relationship_key] = related_obj.to_dict_expand()
        if 'observation' not in inspect(self).unloaded:
            observations = getattr(self, 'observation', [])
            data['Observations'] = [observation.to_dict_expand() for observation in observations]    
        if 'observedArea' in data and data['observedArea'] is not None:
            data['observedArea'] = str(data['observedArea'])
        if 'phenomenonTime' in data:
            data['phenomenonTime'] = self._format_datetime_range(data['phenomenonTime'])
        if 'resultTime' in data:
            data['resultTime'] = self._format_datetime_range(data['resultTime'])
        return data

    def to_dict(self):
        rename_map = {
            'id': '@iot.id',
            'selfLink': '@iot.selfLink',
            'thingNavigationLink': 'Thing@iot.navigationLink',
            'sensorNavigationLink': 'Sensor@iot.navigationLink',
            'observedPropertyNavigationLink': 'ObservedProperty@iot.navigationLink',
            'observationsNavigationLink': 'Observations@iot.navigationLink',
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }  
        if 'observedArea' in data and data['observedArea'] is not None:
            data['observedArea'] = str(data['observedArea'])
        if 'phenomenonTime' in data:
            data['phenomenonTime'] = self._format_datetime_range(data['phenomenonTime'])
        if 'resultTime' in data:
            data['resultTime'] = self._format_datetime_range(data['resultTime'])
        return data

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, 'lower', None)
            upper = getattr(range_obj, 'upper', None)
            return {
                "start": lower.isoformat() if lower else None,
                "end": upper.isoformat() if upper else None
            }
        return None

class FeaturesOfInterest(Base):
    __tablename__ = 'FeaturesOfInterest'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    observationsNavigationLink = Column("Observations@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    encodingType = Column(String(100), nullable=False)
    feature = Column(Geometry(geometry_type='GEOMETRY', srid=4326), nullable=False)
    properties = Column(JSON)
    observation = relationship("Observation", back_populates="featuresofinterest")

    def to_dict_expand(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "observationsNavigationLink": "Observations@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'feature' in data and data['feature'] is not None:
            data['feature'] = str(data['feature'])
        if 'observation' not in inspect(self).unloaded:
            observations = getattr(self, 'observation', [])
            data['Observations'] = [observation.to_dict_expand() for observation in observations] 
        return data

    def to_dict(self):
        rename_map = {
            "id": "@iot.id",
            "selfLink": "@iot.selfLink",
            "observationsNavigationLink": "Observations@iot.navigationLink",
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'feature' in data and data['feature'] is not None:
            data['feature'] = str(data['feature'])
        return data

class Observation(Base):
    __tablename__ = 'Observation'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    selfLink = Column("@iot.selfLink", Text)
    featureOfInterestNavigationLink = Column("FeatureOfInterest@iot.navigationLink", Text)
    datastreamNavigationLink = Column("Datastream@iot.navigationLink", Text)
    phenomenonTime = Column(TIMESTAMP, nullable=False)
    resultTime = Column(TIMESTAMP, nullable=False)
    resultType = Column(Integer, nullable=False)
    resultString = Column(Text)
    resultInteger = Column(Integer)
    resultDouble = Column(Float)
    resultBoolean = Column(Boolean)
    resultJSON = Column(JSON)
    resultQuality = Column(JSON)
    validTime = Column(TIMESTAMP)
    parameters = Column(JSON)
    datastream_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Datastream.id'), nullable=False)
    datastream = relationship("Datastream", back_populates="observation")
    featuresofinterest_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.FeaturesOfInterest.id'), nullable=False)
    featuresofinterest = relationship("FeaturesOfInterest", back_populates="observation")

    def to_dict_expand(self):
        rename_map = {
            'id': '@iot.id',
            'selfLink': '@iot.selfLink',
            'featureOfInterestNavigationLink': 'FeatureOfInterest@iot.navigationLink',
            'datastreamNavigationLink': 'Datastream@iot.navigationLink',
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        for relationships in ['datastream', 'featuresofinterest']:
            if relationships not in inspect(self).unloaded:
                related_obj = getattr(self, relationships, None)
                if related_obj is not None:
                    relationship_key = relationships[0].upper() + relationships[1:]
                    data[relationship_key] = related_obj.to_dict_expand()
        if 'phenomenonTime' in data and data['phenomenonTime'] is not None:
            data['phenomenonTime'] = data['phenomenonTime'].isoformat()
        if 'resultTime' in data and data['resultTime'] is not None:
            data['resultTime'] = data['resultTime'].isoformat()
        if 'validTime' in data and data['validTime'] is not None:
            data['validTime'] = self._format_datetime_range(data['resultTime'])
        if 'resultInteger' in data and data['resultInteger'] is not None:
            data['result'] = data.pop('resultInteger')
            data.pop('resultString')
            data.pop('resultDouble')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultDouble' in data and data['resultDouble'] is not None:
            data['result'] = data.pop('resultDouble')
            data.pop('resultString')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultString' in data and data['resultString'] is not None:
            data['result'] = data.pop('resultString')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultBoolean' in data and data['resultBoolean'] is not None:
            data['result'] = data.pop('resultBoolean')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultString')
            data.pop('resultJSON')
        if 'resultJSON' in data and data['resultJSON'] is not None:
            data['result'] = data.pop('resultJSON')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultString')
        return data

    def to_dict(self):
        rename_map = {
            'id': '@iot.id',
            'selfLink': '@iot.selfLink',
            'featureOfInterestNavigationLink': 'FeatureOfInterest@iot.navigationLink',
            'datastreamNavigationLink': 'Datastream@iot.navigationLink',
        }
        data = {
            rename_map.get(attr.key, attr.key): getattr(self, attr.key)
            for attr in self.__class__.__mapper__.column_attrs
            if attr.key not in inspect(self).unloaded
        }
        if 'phenomenonTime' in data and data['phenomenonTime'] is not None:
            data['phenomenonTime'] = data['phenomenonTime'].isoformat()
        if 'resultTime' in data and data['resultTime'] is not None:
            data['resultTime'] = data['resultTime'].isoformat()
        if 'validTime' in data and data['validTime'] is not None:
            data['validTime'] = self._format_datetime_range(data['resultTime'])
        if 'resultInteger' in data and data['resultInteger'] is not None:
            data['result'] = data.pop('resultInteger')
            data.pop('resultString')
            data.pop('resultDouble')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultDouble' in data and data['resultDouble'] is not None:
            data['result'] = data.pop('resultDouble')
            data.pop('resultString')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultString' in data and data['resultString'] is not None:
            data['result'] = data.pop('resultString')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultJSON')
        if 'resultBoolean' in data and data['resultBoolean'] is not None:
            data['result'] = data.pop('resultBoolean')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultString')
            data.pop('resultJSON')
        if 'resultJSON' in data and data['resultJSON'] is not None:
            data['result'] = data.pop('resultJSON')
            data.pop('resultDouble')
            data.pop('resultInteger')
            data.pop('resultBoolean')
            data.pop('resultString')
        return data

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, 'lower', None)
            upper = getattr(range_obj, 'upper', None)
            return {
                "start": lower.isoformat() if lower else None,
                "end": upper.isoformat() if upper else None
            }
        return None