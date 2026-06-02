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


class Party(Base):
    __tablename__ = "Party"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    thing_navigation_link = Column("Things@iot.navigationLink", Text)
    campaign_navigation_link = Column("Campaigns@iot.navigationLink", Text)
    observationgroup_navigation_link = Column(
        "ObservationGroups@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    role = Column(String(255), nullable=False)
    description = Column(Text)
    display_name = Column("displayName", String(255))
    auth_id = Column("authId", String(255), unique=True)
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    if STAPLUS:
        datastream = relationship("Datastream", back_populates="party")
        thing = relationship("Thing", back_populates="party")
    campaign = relationship("Campaign", back_populates="party")
    observationgroup = relationship("ObservationGroup", back_populates="party")
    commit = relationship("Commit", back_populates="party")
