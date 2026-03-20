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

from app.settings import serverSettings
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

v1 = APIRouter()

@v1.api_route(
    "/Conformance",
    methods=["GET"],
    tags=["Conformance"],
    summary="Get conformance classes",
    description="Returns the list of OGC SensorThings API 1.1 conformance classes implemented by this server. This endpoint is required by the OGC STA 1.1 specification and enables metadata connectors to discover service capabilities programmatically.",
    status_code=status.HTTP_200_OK,
)
async def get_conformance():
    try:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"value": serverSettings["conformance"]},
        )
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Failed to retrieve conformance classes."},
        )
