import argparse
import json
import os
import re
from pathlib import Path

import pandas as pd
import requests

from models import Datastream, Location, Network, ObservedProperty, Sensor, Thing


UOM_MAPPING = {
    "V": "Voltage",
    "°C": "Celsius degree",
    "mm": "Millimeter",
    "%": "Percentage",
    "g": "Gram",
    "m": "Meter",
}


def load_env_file(env_path):
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_environment():
    script_dir = Path(__file__).resolve().parent
    candidate_paths = [
        Path.cwd() / ".env",
        script_dir / ".env",
        script_dir.parent / ".env",
    ]

    seen = set()
    for path in candidate_paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        load_env_file(resolved)


def build_istsos_url():
    explicit_url = os.getenv("ISTSOS4_URL")
    if explicit_url:
        return explicit_url.rstrip("/")

    hostname = os.getenv("HOSTNAME", "http://localhost:8018").rstrip("/")
    subpath = os.getenv("SUBPATH", "/istsos4").strip("/")
    version = os.getenv("VERSION", "/v1.1").strip("/")
    return f"{hostname}/{subpath}/{version}"


def get_credentials():
    username = os.getenv("ISTSOS4_USERNAME") or os.getenv("ISTSOS_ADMIN")
    password = os.getenv("ISTSOS4_PASSWORD") or os.getenv("ISTSOS_ADMIN_PASSWORD")

    if not username or not password:
        raise RuntimeError(
            "Missing istSOS credentials. Set ISTSOS4_USERNAME/ISTSOS4_PASSWORD "
            "or ISTSOS_ADMIN/ISTSOS_ADMIN_PASSWORD in .env."
        )

    return username, password


def normalize_header(value):
    if pd.isna(value):
        return None

    key = str(value).strip().lower()
    if not key:
        return None

    key = re.sub(r"\(.*?\)", "", key)
    key = re.sub(r"[^0-9a-z]+", "_", key)
    return key.strip("_") or None


def clean_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip()
        return value or None

    return value


def parse_json_value(value, default=None):
    value = clean_value(value)
    if value is None:
        return {} if default is None else default

    if isinstance(value, (dict, list)):
        return value

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {} if default is None else default


def parse_definition(value):
    value = clean_value(value)
    if value is None:
        return {}

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def parse_coordinates(value):
    coordinates = clean_value(value)
    if coordinates is None:
        return None

    if isinstance(coordinates, (list, tuple)):
        return [to_number(item) for item in coordinates[:2]]

    text = str(coordinates)
    if text.upper().startswith("POINT"):
        matches = re.findall(r"[-+]?\d*\.?\d+", text)
        if len(matches) >= 2:
            return [to_number(matches[0]), to_number(matches[1])]

    return [to_number(part.strip()) for part in text.split(",")[:2]]


def to_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def split_values(value):
    value = clean_value(value)
    if value is None:
        return []

    return [part.strip() for part in str(value).split(",") if part.strip()]


def get_list_value(values, index, default=None):
    if not values:
        return default

    if index < len(values):
        return values[index]

    return values[-1]


def build_column_names(config_df):
    header_row_2 = config_df.iloc[1]
    header_row_3 = config_df.iloc[2]

    column_names = []
    for idx in range(config_df.shape[1]):
        header_name = normalize_header(header_row_3.iloc[idx])
        if header_name is None:
            header_name = normalize_header(header_row_2.iloc[idx])
        if header_name is None:
            header_name = f"column_{idx}"
        if header_name in column_names:
            header_name = f"{header_name}_{idx}"
        column_names.append(header_name)

    return column_names


def read_configuration(xlsx_path, sheet_name):
    config_df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
    column_names = build_column_names(config_df)

    df = config_df.iloc[3:].copy()
    df.columns = column_names
    df = df.dropna(how="all")
    df["_source_row"] = df.index + 1

    things = df.to_dict("records")
    constraint_keys = [key for key in column_names if key.startswith("constraint_")]
    return things, constraint_keys


def login(server_url, username, password):
    response = requests.post(
        f"{server_url}/Login",
        data={
            "username": username,
            "password": password,
            "grant_type": "password",
        },
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Login failed with status {response.status_code}: {response.text}"
        )

    return response.json()["access_token"]


def require_field(procedure, key):
    value = clean_value(procedure.get(key))
    if value is None:
        row = procedure.get("_source_row", "?")
        raise ValueError(f"Missing required field '{key}' in Excel row {row}.")
    return value


def build_constraints(procedure, constraint_keys):
    constraint_list = []
    for key in constraint_keys:
        constraint = clean_value(procedure.get(key))
        if constraint is None:
            continue
        try:
            constraint_list.append(json.loads(constraint))
        except (TypeError, json.JSONDecodeError):
            constraint_list.append(constraint)
    return constraint_list or None


def build_location_geojson(procedure):
    coordinates = parse_coordinates(require_field(procedure, "location_coordinates"))
    epsg = clean_value(procedure.get("location_epsg"))
    if epsg is None:
        epsg = os.getenv("EPSG", "4326")

    try:
        epsg = int(float(epsg))
    except (TypeError, ValueError):
        pass

    return {
        "type": "Point",
        "coordinates": coordinates,
        "crs": {
            "type": "name",
            "properties": {
                "name": f"EPSG:{epsg}",
            },
        },
    }


def create_entities(procedure, constraint_keys, server_url, token):
    procedure = {key: clean_value(value) for key, value in procedure.items()}
    if not any(value is not None for value in procedure.values()):
        return

    procedure["constraints"] = build_constraints(procedure, constraint_keys)

    location = Location(
        name=require_field(procedure, "location_name"),
        description=procedure.get("location_description") or "",
        location=build_location_geojson(procedure),
        encoding_type="application/json",
    )
    location_id = location.create(server_url, token)

    thing = Thing(
        name=require_field(procedure, "thing_name"),
        description=procedure.get("thing_description") or "",
        properties=parse_json_value(procedure.get("thing_properties"), default={}),
        location_id=location_id,
    )
    thing_id = thing.create(server_url=server_url, token=token)

    sensor_metadata = procedure.get("sensor_metadata")
    model_number = procedure.get("model_number")
    if sensor_metadata is None and model_number is not None:
        sensor_metadata = f"http://example.org/{model_number}.pdf"

    sensor_properties = {
        "modelNumber": model_number,
        "brand": procedure.get("brand"),
        "type": procedure.get("type"),
    }
    sensor_properties = {
        key: value for key, value in sensor_properties.items() if value is not None
    }

    sensor = Sensor(
        name=require_field(procedure, "sensor_name"),
        description=procedure.get("sensor_description") or "",
        metadata=sensor_metadata,
        encoding_type=procedure.get("sensor_encoding_type") or "application/pdf",
        properties=sensor_properties,
    )
    sensor_id = sensor.create(server_url=server_url, token=token)

    observed_property_names = split_values(require_field(procedure, "observed_property_name"))
    observed_property_descriptions = split_values(
        procedure.get("observed_property_description")
    )
    observed_property_definitions = split_values(
        procedure.get("observed_property_definition")
    )
    datastream_names = split_values(procedure.get("datastream_name"))
    datastream_descriptions = split_values(procedure.get("datastream_description"))
    uoms = split_values(procedure.get("datastream_uom"))

    observed_property_ids = []
    for index, op_name in enumerate(observed_property_names):
        observed_property = ObservedProperty(
            name=op_name,
            description=get_list_value(observed_property_descriptions, index, op_name),
            definition=parse_definition(
                get_list_value(observed_property_definitions, index)
            ),
        )
        op_id = observed_property.create(server_url, token=token)
        observed_property_ids.append(op_id)

    for network_name in split_values(require_field(procedure, "network_name")):
        network = Network(name=network_name)
        network_id = network.create(server_url, token=token)

        for index, observed_property_id in enumerate(observed_property_ids):
            uom = get_list_value(uoms, index)

            properties = {
                "samplingFrequency": procedure.get("sampling_frequency"),
                "acquisitionFrequency": procedure.get("acquisition_frequency"),
            }
            properties = {
                key: value for key, value in properties.items() if value is not None
            }

            if procedure["constraints"] is not None:
                properties["constraints"] = procedure["constraints"]

            datastream = Datastream(
                name=get_list_value(
                    datastream_names,
                    index,
                    get_list_value(
                        observed_property_names,
                        index,
                        procedure["sensor_name"],
                    ),
                ),
                description=get_list_value(datastream_descriptions, index, "") or "",
                observation_type=(
                    "http://www.opengis.net/def/observationType/OGC-OM/2.0/"
                    "OM_Measurement"
                ),
                unit_of_measurement={
                    "name": UOM_MAPPING.get(uom, uom),
                    "symbol": uom,
                    "definition": "",
                },
                properties=properties,
                phenomenon_time=require_field(procedure, "datastream_begin"),
                network_id=network_id,
                thing_id=thing_id,
                sensor_id=sensor_id,
                observed_property_id=observed_property_id,
            )
            datastream.create(server_url, token=token)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import sensors and datastreams from an Excel template into istSOS4."
    )
    parser.add_argument("xlsx_path", help="Path to the Excel file to import.")
    parser.add_argument(
        "--sheet-name",
        default="Sheet1",
        help="Worksheet name to read. Defaults to Sheet1.",
    )
    return parser.parse_args()


def main():
    load_environment()
    args = parse_args()

    xlsx_path = Path(args.xlsx_path).expanduser().resolve()
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    server_url = build_istsos_url()
    username, password = get_credentials()
    token = login(server_url, username, password)
    things, constraint_keys = read_configuration(xlsx_path, args.sheet_name)

    for procedure in things:
        create_entities(procedure, constraint_keys, server_url, token)

    print(f"Imported {len(things)} rows into {server_url}.")


if __name__ == "__main__":
    main()
