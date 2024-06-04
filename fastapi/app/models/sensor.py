from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql.json import JSON

class Sensor(Base):
    __tablename__ = 'Sensor'
    __table_args__ = {'schema': SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    sensor_metadata = Column("metadata", JSON, nullable=False)
    properties = Column(JSON)
    datastream = relationship("Datastream", back_populates="sensor")

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "datastream_navigation_link": "Datastreams@iot.navigationLink",
            "encoding_type": "encodingType",
            "sensor_metadata": "metadata",
        }
        return {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }

    def to_dict_expand(self):
        """Serialize the Sensor model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if 'datastream' not in inspect(self).unloaded:
            data['Datastreams'] = [datastream.to_dict_expand() for datastream in self.datastream]
        return data

    def to_dict(self):
        """Serialize the Sensor model to a dict without expanding relationships."""
        return self._serialize_columns()