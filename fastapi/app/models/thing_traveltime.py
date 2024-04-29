from .database import Base, SCHEMA_NAME
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, Text, String
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE

class ThingTravelTime(Base):
    __tablename__ = 'Thing_traveltime'
    
    id = Column(Integer)
    self_link = Column("@iot.selfLink", Text)
    locations_navigation_link = Column("Locations@iot.navigationLink", Text)
    historical_locations_navigation_link = Column("HistoricalLocations@iot.navigationLink", Text)
    datastreams_locations_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    properties = Column(JSON)
    system_time_validity = Column(TSTZRANGE)

    __table_args__ = (PrimaryKeyConstraint(id, system_time_validity), {'schema': SCHEMA_NAME })

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
        if 'system_time_validity' in serialized_data and self.system_time_validity is not None:
            serialized_data['system_time_validity'] = self._format_datetime_range(self.system_time_validity)
        return serialized_data

    def to_dict_expand(self):
        """Serialize the ThingTravelTime model to a dict, excluding 'system_time_validity'."""
        return self._serialize_columns()

    def _format_datetime_range(self, range_obj):
        if range_obj:
            lower = getattr(range_obj, 'lower', None)
            upper = getattr(range_obj, 'upper', None)
            return f"{lower.isoformat()}/{upper.isoformat()}"

        return None