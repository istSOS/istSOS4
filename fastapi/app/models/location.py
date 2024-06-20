from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .database import SCHEMA_NAME, Base
from .location_historicallocation import Location_HistoricalLocation
from .thing_location import Thing_Location


class Location(Base):
    __tablename__ = "Location"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Things@iot.navigationLink", Text)
    historicallocation_navigation_link = Column(
        "HistoricalLocations@iot.navigationLink", Text
    )
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    location = Column(
        Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False
    )
    properties = Column(JSON)
    thing = relationship(
        "Thing", secondary=Thing_Location, back_populates="location"
    )
    historicallocation = relationship(
        "HistoricalLocation",
        secondary=Location_HistoricalLocation,
        back_populates="location",
    )

    def _serialize_columns(self):
        """Serialize model columns to a dict, applying naming transformations."""
        rename_map = {
            "id": "@iot.id",
            "self_link": "@iot.selfLink",
            "thing_navigation_link": "Things@iot.navigationLink",
            "historicallocation_navigation_link": "HistoricalLocations@iot.navigationLink",
            "encoding_type": "encodingType",
        }
        serialized_data = {
            rename_map.get(column.key, column.key): getattr(self, column.key)
            for column in self.__class__.__mapper__.column_attrs
            if column.key not in inspect(self).unloaded
        }
        return serialized_data

    def to_dict_expand(self):
        """Serialize the Location model to a dict, including expanded relationships."""
        data = self._serialize_columns()
        if "thing" not in inspect(self).unloaded:
            data["Things"] = [t.to_dict_expand() for t in self.thing]
        if "historicallocation" not in inspect(self).unloaded:
            data["HistoricalLocations"] = [
                hl.to_dict_expand() for hl in self.historicallocation
            ]
        return data

    def to_dict(self):
        """Serialize the Location model to a dict without expanding relationships."""
        return self._serialize_columns()
