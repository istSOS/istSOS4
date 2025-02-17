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

import os

HOSTNAME = os.getenv("HOSTNAME", "http://localhost:8018")
SUBPATH = os.getenv("SUBPATH", "/istsos4")
VERSION = os.getenv("VERSION", "/v1.1")
DEBUG = int(os.getenv("DEBUG"), 0)
VERSIONING = int(os.getenv("VERSIONING"), 0)
POSTGRES_DB = os.getenv("POSTGRES_DB", "istsos")
ISTSOS_ADMIN = os.getenv("ISTSOS_ADMIN", "admin")
ISTSOS_ADMIN_PASSWORD = os.getenv("ISTSOS_ADMIN_PASSWORD", "admin")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "database")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_PORT_WRITE = os.getenv("POSTGRES_PORT_WRITE")
PG_MAX_OVERFLOW = int(os.getenv("PG_MAX_OVERFLOW", 0))
PG_POOL_SIZE = int(os.getenv("PG_POOL_SIZE", 10))
PG_POOL_TIMEOUT = float(os.getenv("PG_POOL_TIMEOUT", 30))
COUNT_MODE = os.getenv("COUNT_MODE", "FULL")
COUNT_ESTIMATE_THRESHOLD = int(os.getenv("COUNT_ESTIMATE_THRESHOLD", 10000))
TOP_VALUE = int(os.getenv("TOP_VALUE", 100))
PARTITION_CHUNK = int(os.getenv("PARTITION_CHUNK", 10000))
REDIS = int(os.getenv("REDIS"), 0)
EPSG = int(os.getenv("EPSG", 4326))
AUTHORIZATION = int(os.getenv("AUTHORIZATION", 0))
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 5))
ANONYMOUS_VIEWER = int(os.getenv("ANONYMOUS_VIEWER", 0))
