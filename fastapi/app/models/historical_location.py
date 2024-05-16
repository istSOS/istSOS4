from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, Text
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from .location_historicallocation import Location_HistoricalLocation
class HistoricalLocation(Base):
    __tablename__ = 'HistoricalLocation'
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    locations_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    time = Column(TIMESTAMP, nullable=False)
    thing_id = Column(Integer, ForeignKey(f'{SCHEMA_NAME}.Thing.id'), nullable=False)
    thing = relationship("Thing", back_populates="historicallocation")
    location = relationship("Location", secondary=Location_HistoricalLocation, back_populates="historicallocation")

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
        """Serialize the HistoricalLocation model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if 'location' not in inspect(self).unloaded:
            data['Locations'] = [l.to_dict_expand() for l in self.location]
        for relationship in ['thing']:
            if relationship not in inspect(self).unloaded:
                related_obj = getattr(self, relationship, None)
                if related_obj is not None:
                    relationship_key = relationship.capitalize() if relationship != 'location' else 'Locations'
                    data[relationship_key] = related_obj.to_dict_expand()
        return data
        
    def to_dict(self):
        """Serialize the HistoricalLocation model to a dict without expanding relationships."""
        return self._serialize_columns()
