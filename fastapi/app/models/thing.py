from sqlalchemy import Column, Integer, Text, String, ForeignKey
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON
from .database import Base, SCHEMA_NAME

class Thing(Base):
    __tablename__ = 'Thing'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)  # Consider renaming for Pythonic naming, if possible
    locations_navigation_link = Column("Locations@iot.navigationLink", Text)
    historical_locations_navigation_link = Column("HistoricalLocations@iot.navigationLink", Text)
    datastreams_locations_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    properties = Column(JSON)
    location_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Location.id'), nullable=False)
    location = relationship("Location", back_populates="thing")
    datastream = relationship("Datastream", back_populates="thing")
    historicallocation = relationship("HistoricalLocation", back_populates="thing")

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "locations_navigation_link": "Locations@iot.navigationLink",
            "historical_locations_navigation_link": "HistoricalLocations@iot.navigationLink",
            "datastreams_locations_navigation_link": "Datastreams@iot.navigationLink",
        }
        return {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }

    def to_dict_expand(self):
        """Serialize the Thing model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if 'datastream' not in inspect(self).unloaded:
            data['Datastreams'] = [ds.to_dict_expand() for ds in self.datastream]
        if 'historicallocation' not in inspect(self).unloaded:
            data['HistoricalLocations'] = [hl.to_dict_expand() for hl in self.historicallocation]
        if 'location' not in inspect(self).unloaded and self.location is not None:
            data['Location'] = self.location.to_dict_expand()
        return data

    def to_dict(self):
        """Serialize the Thing model to a dict without expanding relationships."""
        return self._serialize_columns()
