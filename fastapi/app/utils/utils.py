import json
from datetime import datetime, timezone

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
                    parser.parse(start_time), parser.parse(end_time)
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
            result_type, column_name = get_result_type_and_column(payload[key])
            payload[column_name] = payload[key]
            payload["resultType"] = result_type
            payload.pop("result")


def get_result_type_and_column(input_string):
    """
    Determines the result type and column name based on the input string.

    Args:
        input_string (str): The input string to evaluate.

    Returns:
        tuple: A tuple containing the result type and column name.

    Raises:
        Exception: If the result cannot be cast to a valid type.
    """
    try:
        value = eval(str(input_string))
    except (SyntaxError, NameError):
        result_type = 0
        column_name = "resultString"
    else:
        if isinstance(value, int):
            result_type = 1
            column_name = "resultInteger"
        elif isinstance(value, float):
            result_type = 2
            column_name = "resultDouble"
        elif isinstance(value, dict):
            result_type = 4
            column_name = "resultJSON"
        else:
            result_type = None
            column_name = None

    if input_string in ["true", "false"]:
        result_type = 3
        column_name = "resultBoolean"

    if result_type is not None:
        return result_type, column_name
    else:
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


async def update_datastream_observedArea_from_obs(conn, datastream_id):
    print("Updating observedArea for datastream", datastream_id)
    # Fetch distinct featuresofinterest IDs associated with the datastream
    query = 'SELECT DISTINCT featuresofinterest_id FROM sensorthings."Observation" WHERE "datastream_id" = $1;'
    featuresofinterest_ids = await conn.fetch(query, datastream_id)

    # Collect the geometries for each feature of interest
    geometries = []
    for foi in featuresofinterest_ids:
        foi_id = foi["featuresofinterest_id"]
        query = 'SELECT feature FROM sensorthings."FeaturesOfInterest" WHERE id = $1;'
        geometry = await conn.fetchval(query, foi_id)

        if geometry:
            geometries.append(geometry)

    if geometries:
        query = f"""
            UPDATE sensorthings."Datastream"
            SET "observedArea" = ST_Force3D(
                ST_ConvexHull(
                    ST_Collect(
                        ARRAY[{', '.join(f"'{g}'::geometry" for g in geometries)}]
                    )
                )
            )
            WHERE id = $1;
        """
    else:
        query = f"""
            UPDATE sensorthings."Datastream"
            SET "observedArea" = NULL
            WHERE id = $1;
        """

    # Execute the update query, passing the datastream_id
    await conn.execute(query, datastream_id)


async def update_datastream_observedArea_from_foi(
    conn, feature_id, delete=False
):
    print(
        "Updating observedArea for datastreams associated with feature",
        feature_id,
    )
    query = 'SELECT DISTINCT datastream_id FROM sensorthings."Observation" WHERE "featuresofinterest_id" = $1;'
    datastream_ids = await conn.fetch(query, feature_id)

    for ds in datastream_ids:
        ds = ds["datastream_id"]

        # Fetch distinct featuresofinterest IDs associated with the datastream
        query = 'SELECT DISTINCT featuresofinterest_id FROM sensorthings."Observation" WHERE "datastream_id" = $1;'
        featuresofinterest_ids = await conn.fetch(query, ds)

        # Collect the geometries for each feature of interest
        geometries = []
        for foi in featuresofinterest_ids:
            foi_id = foi["featuresofinterest_id"]

            if delete:
                if foi_id != feature_id:
                    query = 'SELECT feature FROM sensorthings."FeaturesOfInterest" WHERE id = $1;'
                    geometry = await conn.fetchval(query, foi_id)

                    if geometry:
                        geometries.append(geometry)
            else:
                query = 'SELECT feature FROM sensorthings."FeaturesOfInterest" WHERE id = $1;'
                geometry = await conn.fetchval(query, foi_id)

                if geometry:
                    geometries.append(geometry)

        if geometries:
            query = f"""
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_Force3D(
                    ST_ConvexHull(
                        ST_Collect(
                            ARRAY[{', '.join(f"'{g}'::geometry" for g in geometries)}]
                        )
                    )
                )
                WHERE id = $1;
            """
        else:
            if delete:
                query = f"""
                    UPDATE sensorthings."Datastream"
                    SET "observedArea" = NULL
                    WHERE id = $1;
                """

        # Execute the update query, passing the datastream_id
        await conn.execute(query, ds)
