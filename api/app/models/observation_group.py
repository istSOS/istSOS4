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
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .campaign_observation_group import Campaign_ObservationGroup
from .observation_group_observation import ObservationGroup_Observation
from .relation_observation_group import Relation_ObservationGroup


class ObservationGroup(Base):
    __tablename__ = "ObservationGroup"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    campaign_navigation_link = Column("Campaigns@iot.navigationLink", Text)
    party_navigation_link = Column("Party@iot.navigationLink", Text)
    license_navigation_link = Column("License@iot.navigationLink", Text)
    observation_navigation_link = Column(
        "Observations@iot.navigationLink", Text
    )
    relation_navigation_link = Column("Relations@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    purpose = Column(Text)
    creation_time = Column("creationTime", TIMESTAMP, nullable=False)
    end_time = Column("endTime", TIMESTAMP)
    terms_of_use = Column("termsOfUse", Text)
    privacy_policy = Column("privacyPolicy", Text)
    data_quality = Column("dataQuality", JSON)
    properties = Column(JSON)
    party_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Party.id"))
    license_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.License.id"))
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    campaign = relationship(
        "Campaign",
        secondary=Campaign_ObservationGroup,
        back_populates="observationgroup",
    )
    party = relationship("Party", back_populates="observationgroup")
    license = relationship("License", back_populates="observationgroup")
    observation = relationship(
        "Observation",
        secondary=ObservationGroup_Observation,
        back_populates="observationgroup",
    )
    relation = relationship(
        "Relation",
        secondary=Relation_ObservationGroup,
        back_populates="observationgroup",
    )
    commit = relationship("Commit", back_populates="observationgroup")
