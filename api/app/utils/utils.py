import json

from app import HOSTNAME, TOP_VALUE
from asyncpg.types import Range
from dateutil import parser


def handle_datetime_fields(payload):
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
                if key == "phenomenonTime":
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
