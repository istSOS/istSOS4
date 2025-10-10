# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from app.db.sqlalchemy_db import SCHEMA_NAME, Base
from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text


class Datastream(Base):
    __tablename__ = "Datastream"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    thing_navigation_link = Column("Thing@iot.navigationLink", Text)
    sensor_navigation_link = Column("Sensor@iot.navigationLink", Text)
    observedproperty_navigation_link = Column(
        "ObservedProperty@iot.navigationLink", Text
    )
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    network_navigation_link = Column("Network@iot.navigationLink", Text)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=False)
    unit_of_measurement = Column("unitOfMeasurement", JSON, nullable=False)
    observation_type = Column("observationType", String(100), nullable=False)
    observed_area = Column("observedArea", Geometry)
    phenomenon_time = Column("phenomenonTime", TSTZRANGE)
    result_time = Column("resultTime", TSTZRANGE)
    properties = Column(JSON)
    thing_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Thing.id"),
        nullable=False,
    )
    sensor_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Sensor.id"),
        nullable=False,
    )
    observedproperty_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.ObservedProperty.id"),
        nullable=False,
    )
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    network_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Network.id"))
    thing = relationship("Thing", back_populates="datastream")
    sensor = relationship("Sensor", back_populates="datastream")
    observedproperty = relationship(
        "ObservedProperty", back_populates="datastream"
    )
    observation = relationship("Observation", back_populates="datastream")
    commit = relationship("Commit", back_populates="datastream")
    network = relationship("Network", back_populates="datastream")
