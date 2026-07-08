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
from sqlalchemy.sql.schema import (
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Table,
    UniqueConstraint,
)
from sqlalchemy.sql.sqltypes import Integer
from sqlalchemy.dialects.postgresql.base import TIMESTAMP

ObservationGroup_Observation = Table(
    "ObservationGroup_Observation",
    Base.metadata,
    Column(
        "observationgroup_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.ObservationGroup.id"),
        primary_key=True,
    ),
    Column("observation_id", Integer, primary_key=True),
    Column("phenomenonTime", TIMESTAMP, primary_key=True),
    ForeignKeyConstraint(
        ["observation_id", "phenomenonTime"],
        [
            f"{SCHEMA_NAME}.Observation.id",
            f"{SCHEMA_NAME}.Observation.phenomenonTimeStart",
        ],
    ),
    schema=SCHEMA_NAME,
    table_args__=(
        UniqueConstraint("id", "phenomenonTimeStart"),
        {"schema": SCHEMA_NAME},
    ),
)
