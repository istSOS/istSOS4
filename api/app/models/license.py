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

from app import STAPLUS
from app.db.sqlalchemy_db import SCHEMA_NAME, Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text


class License(Base):
    __tablename__ = "License"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column(
        "Datastreams@iot.navigationLink", Text
    )
    campaign_navigation_link = Column("Campaigns@iot.navigationLink", Text)
    observationgroup_navigation_link = Column(
        "ObservationGroups@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    definition = Column(Text, nullable=False)
    description = Column(Text)
    logo = Column(Text)
    attribution_text = Column("attributionText", Text)
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    if STAPLUS:
        datastream = relationship("Datastream", back_populates="license")
    campaign = relationship("Campaign", back_populates="license")
    observationgroup = relationship(
        "ObservationGroup", back_populates="license"
    )
    commit = relationship("Commit", back_populates="license")
