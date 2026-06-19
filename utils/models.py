# Copyright (C) 2024 Daniele Strigaro IST-SUPSI (www.supsi.ch/ist)
#
# This file is part of things.
#
# things is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# things is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with things.  If not, see <https://www.gnu.org/licenses/>.
import json

import requests


def safe_header_value(value):
    return (
        str(value)
        .replace("’", "'")
        .replace("‘", "'")
        .replace("“", '"')
        .replace("”", '"')
        .replace("–", "-")
        .replace("—", "-")
    )


def escape_odata_string(value):
    return str(value).replace("'", "''")


class Thing:
    def __init__(self, name, description, properties=None, location_id=None):
        self.name = name
        self.description = description
        self.properties = properties
        self.location_id = location_id

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "properties": self.properties,
            "Locations": [{"@iot.id": self.location_id}],
        }

    def create(self, server_url, token):
        # check if already exists
        url = f"{server_url}/Things?$filter=name eq '{self.name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"Thing {self.name} already exists!")
                return r["value"][0]["@iot.id"]

        # Endpoint per creare una Thing
        url = f"{server_url}/Things"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": f"Create {self.name} thing",
        }
        # Converti la Thing in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"Thing {self.name} creata con successo!")
            url = f"{server_url}/Things?$filter=name eq '{self.name}'"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class Datastream:

    def __init__(
        self,
        name,
        description,
        observation_type,
        unit_of_measurement,
        properties,
        phenomenon_time,
        network_id,
        thing_id,
        sensor_id,
        observed_property_id,
    ):
        self.name = name
        self.description = description
        self.observationType = observation_type
        self.unitOfMeasurement = unit_of_measurement
        self.properties = properties
        self.phenomenon_time = phenomenon_time
        self.network_id = network_id
        self.thing_id = thing_id
        self.sensor_id = sensor_id
        self.observed_property_id = observed_property_id

    def to_dict(self):
        data = {
            "name": self.name,
            "description": self.description,
            "observationType": self.observationType,
            "unitOfMeasurement": self.unitOfMeasurement,
            "properties": self.properties,
            "Network": {"@iot.id": self.network_id},
            "Thing": {"@iot.id": self.thing_id},
            "Sensor": {"@iot.id": self.sensor_id},
            "ObservedProperty": {"@iot.id": self.observed_property_id},
            "phenomenonTime": self.phenomenon_time,
        }

        return data

    def create(self, server_url, token):
        # check if already exists
        url = f"{server_url}/Datastreams?$filter=name eq '{self.name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"Datastream {self.name} already exists!")
                return r["value"][0]["@iot.id"]

        # Endpoint per creare un Datastream
        url = f"{server_url}/Datastreams"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": f"Create {self.name} datastream",
        }
        # Converti il Datastream in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"Datastream {self.name} creato con successo!")
            url = f"{server_url}/Datastreams?$filter=name eq '{self.name}'"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class Sensor:

    def __init__(self, name, description, metadata, encoding_type, properties):
        self.name = name
        self.description = description
        self.metadata = metadata
        self.encodingType = encoding_type
        self.properties = properties

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "metadata": self.metadata,
            "encodingType": self.encodingType,
            "properties": self.properties,
        }

    def create(self, server_url, token):
        # check if already exists
        url = f"{server_url}/Sensors?$filter=name eq '{self.name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"Sensor {self.name} already exists!")
                return r["value"][0]["@iot.id"]
        # Endpoint per creare un Sensor
        url = f"{server_url}/Sensors"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": f"Create {self.name} sensor",
        }
        # Converti il Sensor in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"Sensor {self.name} creato con successo!")
            url = f"{server_url}/Sensors?$filter=name eq '{self.name}'"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class ObservedProperty:
    def __init__(self, name, description, definition):
        self.name = name
        self.description = description
        self.definition = definition

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "definition": self.definition,
        }

    def create(self, server_url, token):
        # check if already exists
        safe_name = escape_odata_string(self.name)
        url = f"{server_url}/ObservedProperties?$filter=name eq '{safe_name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"ObservedProperty {self.name} already exists!")
                return r["value"][0]["@iot.id"]

        # Endpoint per creare una ObservedProperty
        url = f"{server_url}/ObservedProperties"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": safe_header_value(
                f"Create {self.name} observed property"
            ),
        }
        # Converti la ObservedProperty in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"ObservedProperty {self.name} creata con successo!")
            safe_name = escape_odata_string(self.name)
            url = f"{server_url}/ObservedProperties?$filter=name eq '{safe_name}'"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class Location:
    def __init__(self, name, description, location, encoding_type):
        self.name = name
        self.description = description
        self.location = location
        self.encodingType = encoding_type

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "location": self.location,
            "encodingType": self.encodingType,
        }

    def create(self, server_url, token):
        # check if already exists
        url = f"{server_url}/Locations?$filter=name eq '{self.name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"Location {self.name} already exists!")
                return r["value"][0]["@iot.id"]

        # Endpoint per creare una Location
        url = f"{server_url}/Locations"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": f"Create {self.name} location",
        }

        # Converti la Location in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"Location {self.name} creata con successo!")
            url = f"{server_url}/Locations?$filter=name eq '{self.name}'"
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class BulkObservation:
    def __init__(self, observations):
        self.observations = observations

    def create(self, server_url, token):
        # Endpoint per creare una Location
        url = f"{server_url}/BulkObservations"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        data = json.dumps(self.observations)
        response = requests.post(url, headers=headers, data=data)

        if response.status_code != 201:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None


class Network:
    def __init__(self, name):
        self.name = name

    def to_dict(self):
        return {
            "name": self.name,
        }

    def create(self, server_url, token):
        # check if already exists
        url = f"{server_url}/Networks?$filter=name eq '{self.name}'"
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            r = response.json()
            if r["value"]:
                print(f"Network {self.name} already exists!")
                return r["value"][0]["@iot.id"]

        # Endpoint per creare una Network
        url = f"{server_url}/Networks"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
            "Commit-message": f"Create {self.name} network",
        }
        # Converti la Network in JSON
        data = json.dumps(self.to_dict())
        # Effettua la richiesta POST all'API SensorThings
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 201:
            print(f"Network {self.name} creata con successo!")
            url = f"{server_url}/Networks?$filter=name eq '{self.name}'"
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                r = response.json()
                return r["value"][0]["@iot.id"]
            else:
                print(f"Errore: {response.status_code}")
                print(response.text)
                return None
        else:
            print(f"Errore: {response.status_code}")
            print(response.text)
            return None
