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
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text


class FeaturesOfInterest(Base):
    __tablename__ = "FeaturesOfInterest"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    encoding_type = Column("encodingType", String(100), nullable=False)
    feature = Column(Geometry, nullable=False)
    properties = Column(JSON)
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    observation = relationship(
        "Observation", back_populates="featuresofinterest"
    )
    commit = relationship("Commit", back_populates="featuresofinterest")
