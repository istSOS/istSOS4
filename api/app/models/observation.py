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
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.dialects.postgresql.ranges import TSTZRANGE
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Boolean, Float, Integer, Text


class Observation(Base):
    __tablename__ = "Observation"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    featuresofinterest_navigation_link = Column(
        "FeatureOfInterest@iot.navigationLink", Text
    )
    datastream_navigation_link = Column("Datastream@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    phenomenon_time = Column("phenomenonTime", TSTZRANGE)
    result_time = Column("resultTime", TIMESTAMP, nullable=False)
    result = Column(JSON)
    result_string = Column("resultString", Text)
    result_number = Column("resultNumber", Float)
    result_boolean = Column("resultBoolean", Boolean)
    result_json = Column("resultJSON", JSON)
    result_quality = Column("resultQuality", JSON)
    valid_time = Column("validTime", TSTZRANGE)
    parameters = Column(JSON)
    featuresofinterest_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.FeaturesOfInterest.id"),
        nullable=False,
    )
    datastream_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Datastream.id"),
        nullable=False,
    )
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    featuresofinterest = relationship(
        "FeaturesOfInterest", back_populates="observation"
    )
    datastream = relationship("Datastream", back_populates="observation")
    commit = relationship("Commit", back_populates="observation")
