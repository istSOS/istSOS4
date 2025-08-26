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
from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, Text


class Commit(Base):
    __tablename__ = "Commit"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    location_navigation_link = Column("Locations@iot.navigationLink", Text)
    thing_navigation_link = Column("Things@iot.navigationLink", Text)
    historicallocation_navigation_link = Column(
        "HistoricalLocations@iot.navigationLink", Text
    )
    observedproperty_navigation_link = Column(
        "ObservedProperties@iot.navigationLink", Text
    )
    sensor_navigation_link = Column("Sensors@iot.navigationLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    featuresofinterest_navigation_link = Column(
        "FeaturesOfInterest@iot.navigationLink", Text
    )
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    network_navigation_link = Column("Networks@iot.navigationLink", Text)
    author = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100))
    message = Column(String(255), nullable=False)
    date = Column(TIMESTAMP, server_default="now()")
    action_type = Column("actionType", String(100), nullable=False)
    location = relationship("Location", back_populates="commit")
    thing = relationship("Thing", back_populates="commit")
    historicallocation = relationship(
        "HistoricalLocation", back_populates="commit"
    )
    observedproperty = relationship(
        "ObservedProperty", back_populates="commit"
    )
    sensor = relationship("Sensor", back_populates="commit")
    datastream = relationship("Datastream", back_populates="commit")
    featuresofinterest = relationship(
        "FeaturesOfInterest", back_populates="commit"
    )
    observation = relationship("Observation", back_populates="commit")
    network = relationship("Network", back_populates="commit")
