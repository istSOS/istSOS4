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

from app import EPSG, HOSTNAME, TOP_VALUE
from asyncpg.types import Range
from dateutil import parser


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
                start_time, end_time = payload[key].split("/")
                payload[key] = Range(
                    parser.parse(start_time),
                    parser.parse(end_time),
                    upper_inc=True,
                )
            else:
                if key == "phenomenonTime" or (
                    datastream and key == "resultTime"
                ):
                    payload[key] = Range(
                        parser.parse(payload[key]),
                        parser.parse(payload[key]),
                        upper_inc=True,
                    )
                else:
                    payload[key] = parser.parse(payload[key])


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
        input_string (str): The input string to evaluate.

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
    elif isinstance(input, int) or isinstance(input, float):
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


def build_nextLink(full_path, count_links):
    nextLink = f"{HOSTNAME}{full_path}"
    new_top_value = TOP_VALUE

    # Handle $top
    if "$top" in nextLink:
        start_index = nextLink.find("$top=") + 5
        end_index = len(nextLink)
        for char in ("&", ";", ")"):
            char_index = nextLink.find(char, start_index)
            if char_index != -1 and char_index < end_index:
                end_index = char_index
        top_value = int(nextLink[start_index:end_index])
        new_top_value = top_value
        nextLink = (
            nextLink[:start_index] + str(new_top_value) + nextLink[end_index:]
        )
    else:
        if "?" in nextLink:
            nextLink = nextLink + f"&$top={new_top_value}"
        else:
            nextLink = nextLink + f"?$top={new_top_value}"

    # Handle $skip
    if "$skip" in nextLink:
        start_index = nextLink.find("$skip=") + 6
        end_index = len(nextLink)
        for char in ("&", ";", ")"):
            char_index = nextLink.find(char, start_index)
            if char_index != -1 and char_index < end_index:
                end_index = char_index
        skip_value = int(nextLink[start_index:end_index])
        new_skip_value = skip_value + new_top_value
        nextLink = (
            nextLink[:start_index] + str(new_skip_value) + nextLink[end_index:]
        )
    else:
        new_skip_value = new_top_value
        nextLink = nextLink + f"&$skip={new_skip_value}"

    # Only return the nextLink if there's more data to fetch
    if new_top_value < count_links:
        return nextLink

    return None


def validate_payload_keys(payload, keys):
    invalid_keys = [key for key in payload.keys() if key not in keys]
    if invalid_keys:
        raise Exception(f"Invalid keys in payload: {', '.join(invalid_keys)}")


def validate_epsg(key):
    crs = key.get("crs")
    if crs is not None:
        epsg_code = int(crs["properties"].get("name").split(":")[1])
        if epsg_code != EPSG:
            raise ValueError(
                f"Invalid EPSG code. Expected {EPSG}, got {epsg_code}"
            )


def handle_associations(payload, keys):
    for key in keys:
        if key in payload:
            if list(payload[key].keys()) != ["@iot.id"]:
                raise Exception(
                    "Invalid format: Each thing dictionary should contain only the '@iot.id' key."
                )
            if key != "FeatureOfInterest":
                payload[f"{key.lower()}_id"] = payload[key]["@iot.id"]
            else:
                payload["featuresofinterest_id"] = payload[key]["@iot.id"]
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
