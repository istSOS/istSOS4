import argparse
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Iterable
import requests

from models import (
    build_istsos_url,
    get_credentials,
    load_environment,
    login,
)

EYEONWATER_DATASTREAMS = [
    "fu_value",
    "fu_observed",
    "fu_processed",
    "hue_angle",
    "p_chla",
    "p_conductivity",
    "p_dissolved_oxygen",
    "p_ph",
    "p_phycocyanin",
    "p_salinity",
    "p_temperature",
    "sd_depth",
    "p_cloud_cover",
    "macrophytes",
]

EYEONWATER_DATASTREAMS_UNITS = {
    "fu_value": {
        "name": "Fluorescence",
        "symbol": "FU",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/FluorescenceUnit",
    },
    "fu_observed": {
        "name": "Forel-Ule observed",
        "symbol": "FU",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/FluorescenceUnit",
    },
    "fu_processed": {
        "name": "Forel-Ule processed",
        "symbol": "FU",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/FluorescenceUnit",
    },
    "hue_angle": {
        "name": "Hue Angle",
        "symbol": "°",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Degree",
    },
    "p_chla": {
        "name": "Chlorophyll-a concentration",
        "symbol": "µg/L",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/MicrogramPerLiter",
    },
    "p_conductivity": {
        "name": "Conductivity",
        "symbol": "µS/cm",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/MicroSiemensPerCentimeter",
    },
    "p_dissolved_oxygen": {
        "name": "Dissolved Oxygen",
        "symbol": "mg/L",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/MilligramPerLiter",
    },
    "p_ph": {
        "name": "pH",
        "symbol": "pH",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/pH",
    },
    "p_phycocyanin": {
        "name": "Phycocyanin concentration",
        "symbol": "µg/L",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/MicrogramPerLiter",
    },
    "p_salinity": {
        "name": "Salinity",
        "symbol": "PSU",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/PracticalSalinityUnit",
    },
    "p_temperature": {
        "name": "Temperature",
        "symbol": "°C",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/DegreeCelsius",
    },
    "sd_depth": {
        "name": "Depth",
        "symbol": "m",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Meter",
    },
    "p_cloud_cover": {
        "name": "Cloud Cover",
        "symbol": "%",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Percent",
    },
    "macrophytes": {
        "name": "Macrophytes",
        "symbol": "count",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Count",
    },
}

EYEONWATER_DATASTREAMS_OBSERVED_PROPERTIES = {
    "fu_value": {
        "name": "Fluorescence",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Fluorescence",
        "description": "Fluorescence value from EyeOnWater.",
    },
    "fu_observed": {
        "name": "Forel-Ule observed",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/ForelUleObserved",
        "description": "Observed Forel-Ule value from EyeOnWater.",
    },
    "fu_processed": {
        "name": "Forel-Ule processed",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/ForelUleProcessed",
        "description": "Processed Forel-Ule value from EyeOnWater.",
    },
    "hue_angle": {
        "name": "Hue Angle",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/HueAngle",
        "description": "Hue angle computed from the image.",
    },
    "p_chla": {
        "name": "Chlorophyll-a concentration",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/ChlorophyllAConcentration",
        "description": "Chlorophyll-a concentration.",
    },
    "p_conductivity": {
        "name": "Conductivity",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Conductivity",
        "description": "Water conductivity.",
    },
    "p_dissolved_oxygen": {
        "name": "Dissolved Oxygen",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/DissolvedOxygenConcentration",
        "description": "Dissolved oxygen concentration.",
    },
    "p_ph": {
        "name": "pH",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/pH",
        "description": "Water pH.",
    },
    "p_phycocyanin": {
        "name": "Phycocyanin concentration",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/PhycocyaninConcentration",
        "description": "Phycocyanin concentration.",
    },
    "p_salinity": {
        "name": "Salinity",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Salinity",
        "description": "Water salinity.",
    },
    "p_temperature": {
        "name": "Temperature",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Temperature",
        "description": "Water temperature.",
    },
    "sd_depth": {
        "name": "Depth",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Depth",
        "description": "Secchi disk depth.",
    },
    "p_cloud_cover": {
        "name": "Cloud Cover",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/CloudCover",
        "description": "Cloud cover percentage.",
    },
    "macrophytes": {
        "name": "Macrophytes",
        "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/MacrophyteCount",
        "description": "Macrophyte count or presence indicator.",
    },
}


class STAClient:
    def __init__(self, base_url, headers=None):
        self.base_url = base_url.rstrip("/")
        self.headers = headers

    def get(self, endpoint, params=None):
        r = requests.get(
            f"{self.base_url}/{endpoint}", params=params, headers=self.headers
        )
        if not r.ok:
            message = r.text.strip()
            raise RuntimeError(
                f"GET {endpoint} failed with status {r.status_code}: {message}"
            )
        data = r.json()
        return data.get("value", data)

    def post(self, endpoint, payload):
        print(f"POST {endpoint} payload:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        r = requests.post(
            f"{self.base_url}/{endpoint}", json=payload, headers=self.headers
        )
        if r.status_code == 409:
            print(f"Skipping {endpoint}: 409 Conflict")
            if r.text.strip():
                print(r.text)
            return None

        if not r.ok:
            message = r.text.strip()
            raise RuntimeError(
                f"POST {endpoint} failed with status {r.status_code}: {message}"
            )

        if r.text.strip():
            return r.json()

        location = r.headers.get("location")
        if not location:
            return {}

        created = requests.get(location, headers=self.headers)
        created.raise_for_status()
        data = created.json()
        return data.get("value", data)

    def get_by_filter(self, endpoint, filter_expr):
        data = self.get(endpoint, {"$filter": filter_expr})
        return data[0] if data else None

    def get_or_create(self, endpoint, filter_expr, payload):
        found = self.get_by_filter(endpoint, filter_expr)
        if found:
            return found

        created = self.post(endpoint, payload)
        if created:
            return created

        return self.get_by_filter(endpoint, filter_expr)


def q(value):
    return str(value).replace("'", "''")


def first_existing(d, *keys):
    for key in keys:
        if isinstance(d, dict) and d.get(key) not in (None, ""):
            return d[key]
    return None


def extract_lat_lon(obs):
    lat = first_existing(
        obs, "lat", "latitude", "photo_latitude", "location_latitude"
    )
    lon = first_existing(
        obs, "lon", "lng", "longitude", "photo_longitude", "location_longitude"
    )

    location = obs.get("location") or {}
    gps = obs.get("gps") or {}
    geometry = obs.get("geometry") or {}

    lat = lat or first_existing(location, "lat", "latitude")
    lon = lon or first_existing(location, "lon", "lng", "longitude")

    lat = lat or first_existing(gps, "lat", "latitude")
    lon = lon or first_existing(gps, "lon", "lng", "longitude")

    if geometry.get("type") == "Point":
        coords = geometry.get("coordinates") or []
        if len(coords) >= 2:
            lon = lon or coords[0]
            lat = lat or coords[1]

    if lat is None or lon is None:
        raise ValueError(
            f"Missing latitude/longitude for EyeOnWater observation {obs.get('id')}"
        )

    return float(lat), float(lon)


def party_payload(obs):
    user = obs.get("user") or {}
    user_id = user.get("user_n_code") or "unknown"
    username = user.get("nickname") or f"EyeOnWater user {user_id}"

    return {
        "role": "individual",
        "displayName": username,
        "authId": f"eyeonwater:{user_id}",
        "description": "Citizen contributor imported from EyeOnWater",
    }


def sensor_payload(obs):
    device = obs.get("device") or {}

    name = (
        first_existing(device, "device_model", "model", "name")
        or "unknown_eyeonwater_device"
    )

    return {
        "name": name,
        "description": "Sensor/device retrieved from EyeOnWater observation",
        "encodingType": "application/json",
        "metadata": json.dumps(device),
    }


def feature_of_interest_payload(obs):
    lat, lon = extract_lat_lon(obs)

    obs_id = obs.get("id") or obs.get("uuid")
    raw_id = obs_id or f"{lat:.7f}_{lon:.7f}"
    foi_id = hashlib.sha1(str(raw_id).encode()).hexdigest()[:12]

    return {
        "name": f"EyeOnWater_FOI_{foi_id}",
        "description": "Sampling location of the EyeOnWater observation",
        "encodingType": "application/vnd.geo+json",
        "feature": {
            "type": "Point",
            "coordinates": [lon, lat],
        },
    }


def datastream_payload(key, thing_id, sensor_id, party_id, network_id):
    return {
        "name": f"EyeOnWaterDatastream_{key}",
        "description": "Datastream retrieved from EyeOnWater observation",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": EYEONWATER_DATASTREAMS_UNITS[key],
        "Thing": {"@iot.id": thing_id},
        "Sensor": {"@iot.id": sensor_id},
        "ObservedProperty": EYEONWATER_DATASTREAMS_OBSERVED_PROPERTIES[key],
        "Party": {"@iot.id": party_id},
        "Network": {"@iot.id": network_id},
    }


def observation_payload(obs, value, datastream_id, foi_id):
    t = obs.get("image", {}).get("date_photo", None)

    if t is None:
        raise ValueError(
            f"Missing date_photo/timestamp for EyeOnWater observation {obs.get('id')}"
        )

    return {
        "phenomenonTime": t,
        "resultTime": t,
        "result": value,
        "resultQuality": "100",
        "Datastream": {"@iot.id": datastream_id},
        "FeatureOfInterest": {"@iot.id": foi_id},
    }


def post_eyeonwater_to_istsos4(
    eyeonwater_json, sta_endpoint, thing_id, network_name, headers=None
):
    client = STAClient(sta_endpoint, headers=headers)
    network = client.get_or_create(
        "Networks",
        f"name eq '{q(network_name)}'",
        {"name": network_name},
    )
    if not network:
        print(
            f"Skipping import: Network conflict could not be resolved for {network_name}"
        )
        return []

    network_id = network["@iot.id"]
    posted = []

    for obs in eyeonwater_json:
        party = party_payload(obs)
        party = client.get_or_create(
            "Parties",
            f"authId eq '{q(party['authId'])}'",
            party,
        )
        if not party:
            print(
                f"Skipping observation {obs.get('id')}: Party conflict could not be resolved"
            )
            continue

        sensor = sensor_payload(obs)
        sensor = client.get_or_create(
            "Sensors",
            f"name eq '{q(sensor['name'])}'",
            sensor,
        )
        if not sensor:
            print(
                f"Skipping observation {obs.get('id')}: Sensor conflict could not be resolved"
            )
            continue

        foi = feature_of_interest_payload(obs)
        foi = client.get_or_create(
            "FeaturesOfInterest",
            f"name eq '{q(foi['name'])}'",
            foi,
        )
        if not foi:
            print(
                f"Skipping observation {obs.get('id')}: FeatureOfInterest conflict could not be resolved"
            )
            continue

        for key, value in (obs.get("water") or {}).items():
            if key not in EYEONWATER_DATASTREAMS:
                continue
            if value is None:
                continue
            if key not in EYEONWATER_DATASTREAMS_UNITS:
                continue
            if key not in EYEONWATER_DATASTREAMS_OBSERVED_PROPERTIES:
                continue

            ds = datastream_payload(
                key=key,
                thing_id=thing_id,
                sensor_id=sensor["@iot.id"],
                party_id=party["@iot.id"],
                network_id=network_id,
            )

            ds = client.get_or_create(
                "Datastreams",
                f"name eq '{q(ds['name'])}'",
                ds,
            )
            if not ds:
                print(
                    f"Skipping {obs.get('id')} {key}: "
                    "Datastream conflict could not be resolved"
                )
                continue

            observation = client.post(
                "Observations",
                observation_payload(
                    obs=obs,
                    value=value,
                    datastream_id=ds["@iot.id"],
                    foi_id=foi["@iot.id"],
                ),
            )
            if not observation:
                print(
                    f"Skipping {obs.get('id')} {key}: Observation already exists"
                )
                continue

            posted.append(
                {
                    "eyeonwater_id": obs.get("id"),
                    "party_id": party["@iot.id"],
                    "sensor_id": sensor["@iot.id"],
                    "feature_of_interest_id": foi["@iot.id"],
                    "datastream_id": ds["@iot.id"],
                    "network_id": network_id,
                    "observation_id": observation.get("@iot.id"),
                    "key": key,
                    "value": value,
                }
            )

    return posted


def read_eyeonwater_json(json_path):
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("value", "results", "observations", "data"):
            observations = data.get(key)
            if isinstance(observations, list):
                return observations

    raise ValueError(
        "EyeOnWater JSON must be a list of observations or contain one in "
        "'value', 'results', 'observations', or 'data'."
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import EyeOnWater observations from JSON into istSOS4."
    )
    parser.add_argument("json_path", help="Path to the EyeOnWater JSON file.")
    parser.add_argument(
        "--thing-id",
        type=int,
        default=os.getenv("EYEONWATER_THING_ID"),
        help="Thing @iot.id to associate with the imported datastreams.",
    )
    parser.add_argument(
        "--network-name",
        default=os.getenv("EYEONWATER_NETWORK_NAME"),
        help="Network name to create or reuse for the imported datastreams.",
    )
    parser.add_argument(
        "--commit-message",
        default="Import EyeOnWater observations",
        help="Commit message sent to istSOS4.",
    )
    return parser.parse_args()


def main():
    load_environment()
    args = parse_args()

    json_path = Path(args.json_path).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"EyeOnWater JSON file not found: {json_path}")

    if args.thing_id is None:
        raise RuntimeError(
            "Missing Thing id. Pass --thing-id or set EYEONWATER_THING_ID in .env."
        )

    if not args.network_name:
        raise RuntimeError(
            "Missing Network name. Pass --network-name or set "
            "EYEONWATER_NETWORK_NAME in .env."
        )

    server_url = build_istsos_url()
    username, password = get_credentials()
    token = login(server_url, username, password)
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "commit-message": args.commit_message,
    }

    data = read_eyeonwater_json(json_path)
    posted = post_eyeonwater_to_istsos4(
        eyeonwater_json=data,
        sta_endpoint=server_url,
        thing_id=args.thing_id,
        network_name=args.network_name,
        headers=headers,
    )

    print(json.dumps(posted, ensure_ascii=False, indent=2))
    print(f"Imported {len(posted)} observations into {server_url}.")


if __name__ == "__main__":
    main()
