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

import json
import re
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from app import EPSG, HOSTNAME, TOP_VALUE, SUBPATH, VERSION
from asyncpg.types import Range
from dateutil import parser

_USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,63}$")

# Entity name mapping to match the existing convention in create_entity
ENTITY_URL_NAMES = {
    "Thing": "Things",
    "Location": "Locations",
    "Sensor": "Sensors",
    "Datastream": "Datastreams",
    "Observation": "Observations",
    "ObservedProperty": "ObservedProperties",
    "FeatureOfInterest": "FeaturesOfInterest",
    "HistoricalLocation": "HistoricalLocations",
    "Network": "Networks",
}


def safe_parse_datetime(value):
    """
    Safely parse a datetime string using dateutil.parser.
    Returns a datetime object or None if parsing fails.

    Args:
        value (Any): Input value to parse.

    Returns:
        Optional[datetime]: Parsed datetime or None if invalid.
    """

    if value is None:
        return None
    try:
        return parser.parse(value)
    except (ValueError, TypeError, OverflowError):
        return None


def extract_iot_id(data):
    """
    Extract and validate the '@iot.id' key from an association dictionary.

    Args:
        data (Dict[str, Any]): Association dictionary containing '@iot.id'.

    Returns:
        int: The validated @iot.id value.

    Raises:
        ValueError: If the structure is invalid or @iot.id is not an integer.
    """

    if not isinstance(data, dict):
        raise ValueError(
            f"Expected dict for association, got {type(data).__name__}"
        )
    if "@iot.id" not in data:
        raise ValueError("Missing '@iot.id' in association payload")

    iot_id = data["@iot.id"]
    if not isinstance(iot_id, int):
        raise ValueError(
            f"Expected int for '@iot.id', got {type(iot_id).__name__}"
        )
    return iot_id


def handle_datetime_fields(payload, datastream=False):
    """
    Converts datetime fields in the payload to datetime objects.

    Args:
        payload (dict): The payload containing the data.

    Returns:
        None
    """
    for key in list(payload.keys()):
        if "time" in key.lower():
            if "/" in payload[key]:
                start_str, end_str = payload[key].split("/", 1)
                start_time = safe_parse_datetime(start_str)
                end_time = safe_parse_datetime(end_str)
                if start_time and end_time:
                    payload[key] = Range(
                        start_time,
                        end_time,
                        upper_inc=True,
                    )
                # Else invalid datetime range
                else:
                    payload[key] = None
            else:
                parsed_time = safe_parse_datetime(payload[key])
                if key == "phenomenonTime" or (
                    datastream and key == "resultTime"
                ):
                    if parsed_time:
                        payload[key] = Range(
                            parsed_time,
                            parsed_time,
                            upper_inc=True,
                        )
                    else:
                        payload[key] = None
                else:
                    payload[key] = parsed_time


def handle_result_field(payload):
    """
    Updates the payload dictionary by handling the 'result' field.

    Args:
        payload (dict): The dictionary containing the payload.

    Returns:
        None
    """
    for key in list(payload.keys()):
        if key == "result":
            result_type, values, columns = get_result_type_and_column(
                payload[key]
            )
            for value, column in zip(values, columns):
                payload[column] = value
            payload["resultType"] = result_type
            payload.pop("result")


def get_result_type_and_column(input):
    """
    Determines the result type and column name based on the input string.

    Args:
        input: The value to evaluate, could be any data type, str, bool, int or float.

    Returns:
        tuple: A tuple containing the result type and column name.

    Raises:
        Exception: If the result cannot be cast to a valid type.
    """

    result_type = None
    values = []
    columns = []
    if isinstance(input, str):
        result_type = 3
        columns.extend(
            ["resultString", "resultBoolean", "resultNumber", "resultJSON"]
        )
        values.extend([input, None, None, None])
    elif isinstance(input, dict):
        result_type = 2
        columns.extend(
            ["resultJSON", "resultBoolean", "resultNumber", "resultString"]
        )
        values.extend([input, None, None, None])
    elif isinstance(input, bool):
        result_type = 1
        columns.extend(
            ["resultBoolean", "resultString", "resultNumber", "resultJSON"]
        )
        values.extend([input, str(input).lower(), None, None])
    elif isinstance(input, (int, float)):
        result_type = 0
        columns.extend(
            ["resultNumber", "resultString", "resultBoolean", "resultJSON"]
        )
        values.extend([input, str(input), None, None])

    if result_type is not None:
        return result_type, values, columns
    raise Exception("Cannot cast result to a valid type")


def response2jsonfile(request, response, filename, body="", status_code=200):
    """
    Writes the response details to a JSON file.

    Args:
        request (Request): The request object.
        response (str): The response message.
        filename (str): The path to the JSON file.
        body (str, optional): The request body. Defaults to "".
        status_code (int, optional): The HTTP status code. Defaults to 200.
    """
    full_path = request.url.path
    r = None
    if request.url.query:
        full_path += "?" + request.url.query
    try:
        with open(filename, "r") as f:
            r = json.load(f)

    except Exception:
        r = []
        pass
    with open(filename, "w") as f:
        r.append(
            {
                "path": full_path,
                "method": request.method,
                "response": response,
                "body": body,
                "status_code": status_code,
            }
        )
        f.write(json.dumps(r, indent=4))


def build_self_link(entity_name, entity_id):
    """
    Build a SensorThings API qualified self-link for an entity.

    Args:
        entity_name: STA entity type e.g. "Datastream", "Thing"
        entity_id: the integer id of the entity

    Returns:
        Full self-link URL string e.g.
        "https://example.org/sta/v1.1/Datastreams(42)"

    Raises:
        ValueError: if entity_name is not a recognised STA entity type
    """
    url_name = ENTITY_URL_NAMES.get(entity_name)
    if url_name is None:
        raise ValueError(
            f"Unknown entity name '{entity_name}'. "
            f"Expected one of: {', '.join(ENTITY_URL_NAMES.keys())}"
        )
    
    return f"{HOSTNAME}{SUBPATH}{VERSION}/{url_name}({entity_id})"


def build_nextLink(full_path, count_links):
    nextLink = f"{HOSTNAME}{full_path}"
    new_top_value = TOP_VALUE

    # Handle $top
    parsed = urlparse(nextLink)

    # Decode query parameters into a dict: {key: [values]}
    query_params = parse_qs(parsed.query, keep_blank_values=True)

    # Handle $top
    if "$top" in query_params:
        top_value = int(query_params["$top"][0])
        query_params["$top"] = [str(top_value)]
        new_top_value = top_value
    else:
        query_params["$top"] = [str(new_top_value)]

    # ---- Handle $skip ----
    if "$skip" in query_params:
        skip_value = int(query_params["$skip"][0])
        query_params["$skip"] = [str(skip_value + new_top_value)]
    else:
        query_params["$skip"] = [str(new_top_value)]

    # nextLink = urlunparse(
    #     parsed._replace(query=urlencode(query_params, doseq=True))
    # )

    new_query = urlencode(query_params, doseq=True, quote_via=quote)

    nextLink = urlunparse(parsed._replace(query=new_query))

    # Only return the nextLink if there's more data to fetch
    if new_top_value < count_links:
        return nextLink

    return None


def validate_payload_keys(payload, keys):
    invalid_keys = [key for key in payload.keys() if key not in keys]
    if invalid_keys:
        raise Exception(f"Invalid keys in payload: {', '.join(invalid_keys)}")


def validate_required_keys(payload, required_keys):
    missing = [key for key in required_keys if key not in payload]
    if missing:
        raise Exception(f"Missing required fields: {', '.join(missing)}")


def validate_epsg(key):
    crs = key.get("crs")
    if crs is not None:
        epsg_code = int(crs["properties"].get("name").split(":")[1])
        if epsg_code != EPSG:
            raise ValueError(
                f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
            )


def handle_associations(payload, keys):
    """
    Safely extract and map association fields to their corresponding *_id properties.

    Args:
        payload (dict): The payload containing associations.
        keys (list): List of association field names to process.

    Raises:
        ValueError: If an association payload is invalid or malformed.
    """

    for key in keys:
        if key in payload:
            try:
                iot_id = extract_iot_id(payload[key])
            except ValueError as e:
                raise ValueError(f"Invalid association for '{key}': {e}")

            if key != "FeatureOfInterest":
                payload[f"{key.lower()}_id"] = iot_id
            else:
                payload["featuresofinterest_id"] = iot_id
            payload.pop(key)


def check_iot_id_in_payload(payload, entity):
    if len(payload) > 1:
        raise ValueError(
            "Invalid payload format: When providing '@iot.id', no other properties should be included."
        )
    if not isinstance(payload["@iot.id"], int):
        raise ValueError(
            f"Expected `{entity} (@iot.id)` to be an `int`, got {type(payload['@iot.id']).__name__}"
        )


def check_missing_properties(payload, required_properties):
    """
    Check if the payload contains all the required properties.

    Args:
        payload (dict): The payload containing the properties.
        required_properties (list): The list of required properties.

    Raises:
        ValueError: If any of the required properties are missing in the payload.

    Returns:
        None
    """

    missing_properties = [
        f"'{prop}'"
        for prop in required_properties
        if f"{prop.lower()}_id" not in payload
    ]
    if missing_properties:
        raise ValueError(
            f"Missing required properties {', '.join(missing_properties)}"
        )


def insert_navigation_link(default_select: dict, key: str, link: str):
    """
    Insert `new navigation_link` into `default_select[key]` after the last *_navigation_link.
    """
    nav_links = [
        i
        for i, v in enumerate(default_select[key])
        if v.endswith("_navigation_link")
    ]
    insert_pos = nav_links[-1] + 1 if nav_links else len(default_select[key])
    default_select[key].insert(insert_pos, link)


def build_expand(expand_node):
    parts = []
    for e in expand_node.identifiers:
        # Start with the base expand identifier
        segment = e.identifier

        subparts = []

        # Handle $select
        if e.subquery and e.subquery.select:
            select_str = ",".join(
                j.name for j in e.subquery.select.identifiers
            )
            subparts.append(f"$select={select_str}")

        # Handle $filter
        if e.subquery and e.subquery.filter:
            subparts.append(f"$filter={e.subquery.filter.filter}")

        # Handle $orderby
        if e.subquery and e.subquery.orderby:
            orderby_str = ",".join(
                f"{j.identifier} {j.order}"
                for j in e.subquery.orderby.identifiers
            )
            subparts.append(f"$orderby={orderby_str}")

        # Handle $top
        if e.subquery and e.subquery.top is not None:
            subparts.append(f"$top={e.subquery.top.count}")

        # Handle $skip
        if e.subquery and e.subquery.skip is not None:
            subparts.append(f"$skip={e.subquery.skip.count}")

        # Handle $count
        if e.subquery and e.subquery.count:
            if e.subquery.count is True:
                subparts.append(f"$count=true")
            else:
                subparts.append(f"$count=false")

        # Handle nested $expand (recursive call)
        if e.subquery and e.subquery.expand:
            nested_expand = build_expand(e.subquery.expand)
            if nested_expand:
                subparts.append(f"$expand={nested_expand}")

        # If we collected subparts, wrap them in parentheses after the identifier
        if subparts:
            segment += f"({';'.join(subparts)})"

        parts.append(segment)

    return ",".join(parts)


def validate_username(username: str) -> bool:
    """Return True if *username* contains only letters, digits and underscores
    and is between 3 and 63 characters long."""
    return bool(_USERNAME_RE.match(username))


def pg_quote_ident(name: str) -> str:
    """Safely double-quote a PostgreSQL identifier (role name, username, etc.).

    Doubles any embedded double-quote characters and wraps the result in
    double quotes, matching the behaviour of PostgreSQL's quote_ident().
    """
    return '"' + name.replace('"', '""') + '"'


def pg_quote_literal(val: str) -> str:
    """Safely single-quote a PostgreSQL string literal.

    Doubles any embedded single-quote characters and wraps the result in
    single quotes, matching the behaviour of PostgreSQL's quote_literal().
    """
    return "'" + val.replace("'", "''") + "'"
