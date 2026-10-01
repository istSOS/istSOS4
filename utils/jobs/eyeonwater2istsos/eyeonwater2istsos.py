import argparse
import hashlib
import json
import os
from pathlib import Path

import requests
from istsos4_client import (
    Client,
    FeatureOfInterest,
    Network,
    Observation,
    ObservedProperty,
    Sensor,
    staplus,
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


def is_conflict(exc):
    return exc.response is not None and exc.response.status_code == 409


def get_or_create(client, entity, filter_expr, commit_message=None):
    """Reuse the first entity matching filter_expr, else create it.

    Returns None when the create hits a 409 the filter cannot see.
    """
    entity_type = type(entity)
    found = next(client.iter_list(entity_type, filter=filter_expr, top=1), None)
    if found:
        return found

    try:
        client.post(entity, commit_message=commit_message)
    except requests.HTTPError as exc:
        if not is_conflict(exc):
            raise
        print(f"Skipping {entity_type.ENDPOINT}: 409 Conflict")
        return next(
            client.iter_list(entity_type, filter=filter_expr, top=1), None
        )
    return entity


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

    return staplus.Party(
        role="individual",
        display_name=username,
        auth_id=f"eyeonwater:{user_id}",
        description="Citizen contributor imported from EyeOnWater",
    )


def sensor_payload(obs):
    device = obs.get("device") or {}

    name = (
        first_existing(device, "device_model", "model", "name")
        or "unknown_eyeonwater_device"
    )

    return Sensor(
        name=name,
        description="Sensor/device retrieved from EyeOnWater observation",
        encoding_type="application/json",
        metadata=json.dumps(device),
    )


def feature_of_interest_payload(obs):
    lat, lon = extract_lat_lon(obs)

    obs_id = obs.get("id") or obs.get("uuid")
    raw_id = obs_id or f"{lat:.7f}_{lon:.7f}"
    foi_id = hashlib.sha1(str(raw_id).encode()).hexdigest()[:12]

    return FeatureOfInterest(
        name=f"EyeOnWater_FOI_{foi_id}",
        description="Sampling location of the EyeOnWater observation",
        encoding_type="application/vnd.geo+json",
        feature={
            "type": "Point",
            "coordinates": [lon, lat],
        },
    )


def datastream_payload(key, thing_id, sensor_id, party_id, network_id):
    return staplus.Datastream(
        name=f"EyeOnWaterDatastream_{key}",
        description="Datastream retrieved from EyeOnWater observation",
        observation_type="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        unit_of_measurement=EYEONWATER_DATASTREAMS_UNITS[key],
        thing=thing_id,
        sensor=sensor_id,
        observed_property=ObservedProperty(
            **EYEONWATER_DATASTREAMS_OBSERVED_PROPERTIES[key]
        ),
        party=party_id,
        network=network_id,
    )


def observation_payload(obs, value, datastream_id, foi_id):
    t = obs.get("image", {}).get("date_photo", None)

    if t is None:
        raise ValueError(
            f"Missing date_photo/timestamp for EyeOnWater observation {obs.get('id')}"
        )

    return Observation(
        phenomenon_time=t,
        result_time=t,
        result=value,
        result_quality="100",
        datastream=datastream_id,
        feature_of_interest=foi_id,
    )


def post_eyeonwater_to_istsos4(
    eyeonwater_json, client, thing_id, network_name, commit_message=None
):
    network = get_or_create(
        client,
        Network(name=network_name),
        f"name eq '{q(network_name)}'",
        commit_message,
    )
    if not network:
        print(
            f"Skipping import: Network conflict could not be resolved for {network_name}"
        )
        return []

    network_id = network.iot_id
    posted = []

    for obs in eyeonwater_json:
        party = party_payload(obs)
        party = get_or_create(
            client,
            party,
            f"authId eq '{q(party.auth_id)}'",
            commit_message,
        )
        if not party:
            print(
                f"Skipping observation {obs.get('id')}: Party conflict could not be resolved"
            )
            continue

        sensor = sensor_payload(obs)
        sensor = get_or_create(
            client,
            sensor,
            f"name eq '{q(sensor.name)}'",
            commit_message,
        )
        if not sensor:
            print(
                f"Skipping observation {obs.get('id')}: Sensor conflict could not be resolved"
            )
            continue

        foi = feature_of_interest_payload(obs)
        foi = get_or_create(
            client,
            foi,
            f"name eq '{q(foi.name)}'",
            commit_message,
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
                sensor_id=sensor.iot_id,
                party_id=party.iot_id,
                network_id=network_id,
            )

            ds = get_or_create(
                client,
                ds,
                f"name eq '{q(ds.name)}'",
                commit_message,
            )
            if not ds:
                print(
                    f"Skipping {obs.get('id')} {key}: "
                    "Datastream conflict could not be resolved"
                )
                continue

            observation = observation_payload(
                obs=obs,
                value=value,
                datastream_id=ds.iot_id,
                foi_id=foi.iot_id,
            )
            try:
                client.post(observation, commit_message=commit_message)
            except requests.HTTPError as exc:
                if not is_conflict(exc):
                    raise
                print(
                    f"Skipping {obs.get('id')} {key}: Observation already exists"
                )
                continue

            posted.append(
                {
                    "eyeonwater_id": obs.get("id"),
                    "party_id": party.iot_id,
                    "sensor_id": sensor.iot_id,
                    "feature_of_interest_id": foi.iot_id,
                    "datastream_id": ds.iot_id,
                    "network_id": network_id,
                    "observation_id": observation.iot_id,
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


def connect():
    return Client(
        os.environ["ISTSOS4_URL"],
        os.environ["ISTSOS4_USERNAME"],
        os.environ["ISTSOS4_PASSWORD"],
        staplus=True,
    )


def main():
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

    client = connect()
    data = read_eyeonwater_json(json_path)
    posted = post_eyeonwater_to_istsos4(
        eyeonwater_json=data,
        client=client,
        thing_id=args.thing_id,
        network_name=args.network_name,
        commit_message=args.commit_message,
    )

    print(json.dumps(posted, ensure_ascii=False, indent=2))
    print(f"Imported {len(posted)} observations into {client.base_url}.")


if __name__ == "__main__":
    main()
