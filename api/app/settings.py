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

tables = [
    "Datastreams",
    "FeaturesOfInterest",
    "HistoricalLocations",
    "Locations",
    "Observations",
    "ObservedProperties",
    "Sensors",
    "Things",
]
serverSettings = {
    "conformance": [
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/thing",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/location",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/historical-location",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/datastream",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/sensor",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observed-property",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observation",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/feature-of-interest",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/entity-control-information",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/resource-path",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/order",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/expand",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/select",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/orderby",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/skip",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/top",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/filter",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/built-in-filter-operations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/create-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/link-to-existing-entities",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/deep-insert",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/deep-insert-status-code",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/update-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/delete-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/update-entity-jsonpatch",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/data-array/data-array",
    ],
}
