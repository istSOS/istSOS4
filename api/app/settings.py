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
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/thing/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/thing/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/location/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/location/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/historical-location/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/historical-location/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/datastream/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/datastream/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/sensor/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/sensor/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observed-property/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observed-property/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observation/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/observation/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/feature-of-interest/properties",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/feature-of-interest/relations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel/entity-control-information/common-control-information",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/resource-path/resource-path-to-entities",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/order",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/expand",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/select",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/status-code",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/query-status-code",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/orderby",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/top",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/skip",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/count",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/filter",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/built-in-filter-operations",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/built-in-query-functions",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/request-data/pagination",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/create-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/link-to-existing-entities",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/deep-insert",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/deep-insert-status-code",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/update-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/delete-entity",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/historical-location-auto-creation",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/update-entity-put",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/create-update-delete/update-entity-jsonpatch",
        "http://www.opengis.net/spec/iot_sensing/1.1/req/data-array/data-array",
    ],
}
