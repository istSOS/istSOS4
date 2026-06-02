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
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .campaign_datastream import Campaign_Datastream
from .campaign_observation_group import Campaign_ObservationGroup


class Campaign(Base):
    __tablename__ = "Campaign"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    party_navigation_link = Column("Party@iot.navigationLink", Text)
    license_navigation_link = Column("License@iot.navigationLink", Text)
    datastream_navigation_link = Column("Datastreams@iot.navigationLink", Text)
    observationgroup_navigation_link = Column(
        "ObservationGroups@iot.navigationLink", Text
    )
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    classification = Column(String(255))
    terms_of_use = Column("termsOfUse", Text, nullable=False)
    privacy_policy = Column("privacyPolicy", Text)
    creation_time = Column("creationTime", TIMESTAMP, nullable=False)
    start_time = Column("startTime", TIMESTAMP)
    end_time = Column("endTime", TIMESTAMP)
    url = Column(Text)
    party_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Party.id"))
    license_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.License.id"))
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    party = relationship("Party", back_populates="campaign")
    license = relationship("License", back_populates="campaign")
    datastream = relationship(
        "Datastream",
        secondary=Campaign_Datastream,
        back_populates="campaign",
    )
    observationgroup = relationship(
        "ObservationGroup",
        secondary=Campaign_ObservationGroup,
        back_populates="campaign",
    )
    commit = relationship("Commit", back_populates="campaign")
