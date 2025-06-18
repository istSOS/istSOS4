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
from sqlalchemy.sql.schema import Column, ForeignKey, Table
from sqlalchemy.sql.sqltypes import Integer

Thing_Location = Table(
    "Thing_Location",
    Base.metadata,
    Column(
        "thing_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Thing.id"),
        primary_key=True,
    ),
    Column(
        "location_id",
        Integer,
        ForeignKey(f"{SCHEMA_NAME}.Location.id"),
        primary_key=True,
    ),
    schema=SCHEMA_NAME,
)
