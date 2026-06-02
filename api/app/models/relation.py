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
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text

from .relation_observation_group import Relation_ObservationGroup


class Relation(Base):
    __tablename__ = "Relation"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(Integer, primary_key=True)
    self_link = Column("@iot.selfLink", Text)
    observationgroup_navigation_link = Column(
        "ObservationGroups@iot.navigationLink", Text
    )
    subject_navigation_link = Column("Subject@iot.navigationLink", Text)
    object_navigation_link = Column("Object@iot.navigationLink", Text)
    commit_navigation_link = Column("Commit@iot.navigationLink", Text)
    role = Column(String(255), nullable=False)
    description = Column(Text)
    properties = Column(JSON)
    subject_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Observation.id"),
        nullable=False,
    )
    object_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Observation.id"))
    external_resource = Column("externalResource", Text)
    commit_id = Column(Integer, ForeignKey(f"{SCHEMA_NAME}.Commit.id"))
    observationgroup = relationship(
        "ObservationGroup",
        secondary=Relation_ObservationGroup,
        back_populates="relation",
    )
    subject = relationship(
        "Observation", foreign_keys=[subject_id], back_populates="objects"
    )
    object = relationship(
        "Observation", foreign_keys=[object_id], back_populates="subjects"
    )
    commit = relationship("Commit", back_populates="relation")
