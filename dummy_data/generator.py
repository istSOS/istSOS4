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

"""
Dummy data generator and FROST migration utility for istSOS4.

MODES (controlled by RUN_MIGRATION in .env or environment):

  RUN_MIGRATION=0 (default)
    Generates lightweight synthetic dummy data directly into the database.
    Controlled by N_THINGS, N_OBSERVED_PROPERTIES, INTERVAL, FREQUENCY, etc.
    Use this for quick local dev and testing.

  RUN_MIGRATION=1
    Fetches the full entity tree from a Fraunhofer FROST SensorThings API
    endpoint and inserts it into the istSOS deployment via direct postgres
    writes. Wipes existing data before inserting.
    Tested against: https://airquality-frost.k8s.ilt-dmz.iosb.fraunhofer.de/v1.1
    (5610 Things, ~23k Datastreams, 9 ObservedProperties)

IMPORTANT before running migration:
  - Set NETWORK=0 in .env (FROST data has no network concept)
  - Set FROST_BASE_URL if targeting a different STA endpoint
  - Migration runs once; use `docker compose down` without -v to preserve data

USAGE:
  # automatic via docker compose (reads RUN_MIGRATION from .env)
  docker compose -f dev_docker-compose.yml up -d

  # manual one-shot run against a live stack
  docker run --rm --env-file .env -e RUN_MIGRATION=1 -e POSTGRES_HOST=istsos4-database --network istsos4-network istsos4-dummy_data python3 generator.py
"""

import asyncio
import json
import os
import random
from datetime import datetime, time
from typing import Any, Optional

import aiohttp
import asyncpg
import isodate
from asyncpg.types import Range

hostname = os.getenv("HOSTNAME", "http://localhost:8018")
subpath = os.getenv("SUBPATH", "/istsos4")
version = os.getenv("VERSION", "/v1.1")
versioning = int(os.getenv("VERSIONING"), 0)
pg_db = os.getenv("POSTGRES_DB", "istsos")
pg_user = os.getenv("ISTSOS_ADMIN", "admin")
pg_password = os.getenv("ISTSOS_ADMIN_PASSWORD", "admin")
pg_host = os.getenv("POSTGRES_HOST", "database")
pg_port = os.getenv("POSTGRES_PORT", "5432")
pg_write_port = os.getenv("POSTGRES_PORT_WRITE")
create_dummy_data = int(os.getenv("DUMMY_DATA", 1))
delete_dummy_data = int(os.getenv("CLEAR_DATA", 0))
n_things = int(os.getenv("N_THINGS", 10))
n_observed_properties = int(os.getenv("N_OBSERVED_PROPERTIES", 2))
interval = isodate.parse_duration(os.getenv("INTERVAL", "P1Y"))
frequency = isodate.parse_duration(os.getenv("FREQUENCY", "PT5M"))
date = datetime.strptime(
    os.getenv(
        "START_DATETIME", datetime.combine(datetime.now().today(), time.min)
    ),
    "%Y-%m-%dT%H:%M:%S.%f%z",
)
chunk = isodate.parse_duration(os.getenv("CHUNK_INTERVAL", "P1Y"))
epsg = int(os.getenv("EPSG", 4326))
authorization = int(os.getenv("AUTHORIZATION", 0))
st_aggregate = os.getenv("ST_AGGREGATE", "CONVEX_HULL")

run_migration = os.getenv("RUN_MIGRATION", "0") == "1"
frost_base_url = os.getenv(
    "FROST_BASE_URL",
    "https://airquality-frost.k8s.ilt-dmz.iosb.fraunhofer.de/v1.1",
)
frost_page_size = int(os.getenv("FROST_PAGE_SIZE", 5613))
frost_timeout = float(os.getenv("FROST_TIMEOUT", 60.0))
network = int(os.getenv("NETWORK", 0))

observedProperties = []


async def get_pool():
    """
    Retrieves or creates a connection pool to the PostgreSQL database.

    Returns:
        asyncpg.pool.Pool: The connection pool object.
    """

    if pg_write_port:
        return await asyncpg.create_pool(
            dsn=f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_write_port}/{pg_db}"
        )
    return await asyncpg.create_pool(
        dsn=f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )


async def get_user(conn):
    query = """
        SELECT id, uri
        FROM sensorthings."User"
        WHERE username = $1;
    """
    return await conn.fetchrow(query, pg_user)


async def generate_commit(conn, user_id, user_uri):
    if authorization:
        query = """
            INSERT INTO sensorthings."Commit" (author, "encodingType", message, "actionType", user_id)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id;
        """
        return await conn.fetchval(
            query, user_uri, "text/plain", "dummy data", "CREATE", user_id
        )
    else:
        query = """
            INSERT INTO sensorthings."Commit" (author, "encodingType", message, "actionType")
            VALUES ($1, $2, $3, $4)
            RETURNING id;
        """
        return await conn.fetchval(
            query, user_uri, "text/plain", "dummy data", "CREATE"
        )


async def generate_networks(conn, commit_id):
    """
    Generate two different networks and insert them into the database.

    Args:
        conn: The asyncpg connection object.
        commit_id: The commit id to associate with the networks (optional).

    Returns:
        List of inserted network IDs.
    """

    names = random.sample(["psos", "acsot", "defmin"], 2)

    inserted_ids = []
    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."Network" (name, commit_id)
            VALUES ($1, $2)
            RETURNING id;
        """
        for name in names:
            row = await conn.fetchrow(insert_sql, name, commit_id)
            inserted_ids.append(row["id"])
    else:
        insert_sql = """
            INSERT INTO sensorthings."Network" (name)
            VALUES ($1)
            RETURNING id;
        """
        for name in names:
            row = await conn.fetchrow(insert_sql, name)
            inserted_ids.append(row["id"])

    return inserted_ids


async def generate_things(conn, commit_id):
    """
    Generate a list of things and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    things = []
    for i in range(1, n_things + 1):
        description = f"thing {i}"
        name = f"thing_name_{i}"
        properties = json.dumps({"reference": f"{i}"})
        if commit_id is not None:
            things.append((description, name, properties, commit_id))
        else:
            things.append((description, name, properties))

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."Thing" (description, name, properties, commit_id)
            VALUES ($1, $2, $3, $4)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."Thing" (description, name, properties)
            VALUES ($1, $2, $3)
        """

    await conn.executemany(insert_sql, things)


async def generate_locations(conn, commit_id):
    """
    Generate locations and insert them into the database.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """

    locations = []
    for i in range(1, n_things + 1):
        lon = random.uniform(-180, 180)
        lat = random.uniform(-90, 90)
        # elevation = random.uniform(0, 1000)

        description = f"location {i}"
        name = f"location_name_{i}"
        location = f"SRID={epsg};POINT({lon} {lat})"
        encodingType = "application/geo+json"
        if commit_id is not None:
            locations.append(
                (description, name, location, encodingType, commit_id)
            )
        else:
            locations.append((description, name, location, encodingType))

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."Location" (description, name, location, "encodingType", commit_id)
            VALUES ($1, $2, $3::public.geometry, $4, $5)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."Location" (description, name, location, "encodingType")
            VALUES ($1, $2, $3::public.geometry, $4)
        """
    await conn.executemany(insert_sql, locations)


async def generate_things_locations(conn):
    """
    Generate and insert things locations into the database.

    Parameters:
    cur (cursor): The database cursor object.

    Returns:
    None
    """

    things_locations = []
    for i in range(1, n_things + 1):
        thing_id = i
        location_id = i
        things_locations.append((thing_id, location_id))

    insert_sql = """
    INSERT INTO sensorthings."Thing_Location" (thing_id, location_id)
    VALUES ($1, $2)
    """
    await conn.executemany(insert_sql, things_locations)


async def generate_historicallocations(conn, commit_id):
    """
    Generate historical locations for things and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    historicallocations = []
    time = date
    for i in range(1, n_things + 1):
        time += frequency
        thing_id = i
        if commit_id is not None:
            historicallocations.append((time, thing_id, commit_id))
        else:
            historicallocations.append((time, thing_id))

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."HistoricalLocation" (time, thing_id, commit_id)
            VALUES ($1, $2, $3)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."HistoricalLocation" (time, thing_id)
            VALUES ($1, $2)
        """
    await conn.executemany(insert_sql, historicallocations)


async def generate_locations_historicallocations(conn):
    """
    Generate a list of tuples representing the relationship between location and historical location.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """

    locations_historicallocations = []
    for i in range(1, n_things + 1):
        location_id = i
        historicallocation_id = i
        locations_historicallocations.append(
            (location_id, historicallocation_id)
        )

    insert_sql = """
    INSERT INTO sensorthings."Location_HistoricalLocation" (location_id, historicallocation_id)
    VALUES ($1, $2)
    """
    await conn.executemany(insert_sql, locations_historicallocations)


mapping_op = {
    "meteo:air:rainfall": ["P", "Millimeter", "mm"],
    "meteo:air:temperature": ["T", "Celsius degree", "°C"],
    "meteo:air:humidity": ["H", "Percentage", "%"],
}


async def generate_observedProperties(conn, commit_id):
    """
    Generate observed properties and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    keys = list(mapping_op)
    for i in range(1, n_observed_properties + 1):
        key = keys[(i - 1) % len(keys)]
        name = f"{key}_{i}"
        definition = "{}"
        description = key.replace(":", " ")
        if commit_id is not None:
            observedProperties.append(
                (name, definition, description, commit_id)
            )
        else:
            observedProperties.append((name, definition, description))

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."ObservedProperty" (name, definition, description, commit_id)
            VALUES ($1, $2, $3, $4)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."ObservedProperty" (name, definition, description)
            VALUES ($1, $2, $3)
        """
    await conn.executemany(insert_sql, observedProperties)


async def generate_sensors(conn, commit_id):
    """
    Generate a list of sensors with random descriptions, names, encoding types, and metadata.

    Args:
        cur (cursor): The database cursor object.

    Returns:
        None
    """

    sensors = []
    for i in range(1, n_things * n_observed_properties + 1):
        description = f"sensor {i}"
        name = f"sensor_name_{i}"
        encodingType = "application/pdf"
        metadata = f"{random.choice(['Temperature', 'Humidity', 'Pressure', 'Light', 'CO2', 'Motion'])} sensor"
        if commit_id is not None:
            sensors.append(
                (description, name, encodingType, metadata, commit_id)
            )
        else:
            sensors.append((description, name, encodingType, metadata))

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."Sensor" (description, name, "encodingType", metadata, commit_id)
            VALUES ($1, $2, $3, $4, $5)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."Sensor" (description, name, "encodingType", metadata)
            VALUES ($1, $2, $3, $4)
        """
    await conn.executemany(insert_sql, sensors)


async def generate_datastreams(conn, commit_id, network_ids):
    """
    Generate datastreams and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    datastreams = []
    cnt = 1
    for i in range(1, n_things + 1):
        for j in range(1, n_observed_properties + 1):

            datastream = {
                "unitOfMeasurement": json.dumps(
                    {
                        "name": mapping_op[
                            observedProperties[j - 1][0].rsplit("_", 1)[0]
                        ][1],
                        "symbol": mapping_op[
                            observedProperties[j - 1][0].rsplit("_", 1)[0]
                        ][2],
                        "definition": "",
                    }
                ),
                "properties": json.dumps(
                    {
                        "resolution": "PT10M",
                        "qualityIndexLimits": {"max": "", "min": ""},
                        "acquisitionInterval": "PT10M",
                    }
                ),
                "description": f"datastream {cnt}",
                "name": f"{mapping_op[
                            observedProperties[j - 1][0].rsplit("_", 1)[0]
                        ][0]}_datastream_{cnt}",
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "thing_id": i,
                "sensor_id": cnt,
                "observedproperty_id": j,
            }
            if commit_id is not None:
                datastream["commit_id"] = commit_id

            if network_ids:
                datastream["network_id"] = random.choice(network_ids)

            datastreams.append(datastream)
            cnt += 1

    keys = ", ".join(f'"{key}"' for key in datastream.keys())
    values_placeholders = ", ".join(f"${i+1}" for i in range(len(datastream)))

    insert_sql = f"""
        INSERT INTO sensorthings."Datastream" ({keys})
        VALUES ({values_placeholders})
    """
    for datastream in datastreams:
        await conn.execute(insert_sql, *datastream.values())


async def generate_featuresofinterest(conn, commit_id):
    """
    Generate features of interest and insert them into the database.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """

    featuresofinterest = []
    for i in range(1, n_things + 1):
        lon = random.uniform(-180, 180)
        lat = random.uniform(-90, 90)
        # elevation = random.uniform(0, 1000)

        description = f"featuresofinterest {i}"
        name = f"featuresofinterest_name_{i}"
        encodingType = "application/geo+json"
        feature = f"SRID={epsg};POINT({lon} {lat})"
        if commit_id is not None:
            featuresofinterest.append(
                (description, name, encodingType, feature, commit_id)
            )
        else:
            featuresofinterest.append(
                (description, name, encodingType, feature)
            )

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."FeaturesOfInterest" (description, name, "encodingType", feature, commit_id)
            VALUES ($1, $2, $3, $4::public.geometry, $5)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."FeaturesOfInterest" (description, name, "encodingType", feature)
            VALUES ($1, $2, $3, $4::public.geometry)
        """
    await conn.executemany(insert_sql, featuresofinterest)


async def insert_observations(conn, observations, commit_id):
    cols = [
        "phenomenonTime",
        "resultNumber",
        "resultType",
        "datastream_id",
        "featuresofinterest_id",
    ]

    if commit_id is not None:
        cols.append("commit_id")

    column_names = ", ".join(f'"{col}"' for col in cols)

    values_placeholders = ", ".join(
        f"({', '.join(['$' + str(i + 1 + j * len(observations[0])) for i in range(len(observations[0]))])})"
        for j in range(len(observations))
    )

    query = f"""
        INSERT INTO sensorthings."Observation"
        ({column_names})
        VALUES {values_placeholders};
    """

    flattened_values = [
        item for observation in observations for item in observation
    ]

    await conn.execute(query, *flattened_values)


async def update_datastream_phenomenon_time(conn, observations, datastream_id):
    phenomenon_times = [record[0].lower for record in observations]

    update_sql = """
        UPDATE sensorthings."Datastream"
        SET "phenomenonTime" = tstzrange(
            LEAST($1::timestamptz, lower("phenomenonTime")),
            GREATEST($2::timestamptz, upper("phenomenonTime")),
            '[]'
        )
        WHERE id = $3::bigint
    """
    await conn.execute(
        update_sql,
        min(phenomenon_times),
        max(phenomenon_times),
        datastream_id,
    )


async def update_datastream_observed_area(conn):
    query = 'SELECT DISTINCT id FROM sensorthings."Datastream";'
    datastream_ids = await conn.fetch(query)
    for ds in datastream_ids:
        ds = ds["id"]
        # Fetch distinct featuresofinterest IDs associated with the datastream
        query = 'SELECT DISTINCT featuresofinterest_id FROM sensorthings."Observation" WHERE "datastream_id" = $1;'
        featuresofinterest_ids = await conn.fetch(query, ds)

        # Collect the geometries for each feature of interest
        geometries = []
        for foi in featuresofinterest_ids:
            foi_id = foi["featuresofinterest_id"]

            # Fetch the actual geometry associated with the feature of interest
            query = 'SELECT feature FROM sensorthings."FeaturesOfInterest" WHERE id = $1;'
            geometry = await conn.fetchval(query, foi_id)

            if geometry:
                geometries.append(geometry)

        if geometries:
            if st_aggregate == "CONVEX_HULL":
                query = f"""
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_ConvexHull(
                    ST_Collect(
                        ARRAY[{', '.join(f"'{g}'::geometry" for g in geometries)}]
                    )
                )
                WHERE id = $1;
                """
            else:
                query = f"""
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_Envelope(
                    ST_Collect(
                        ARRAY[{', '.join(f"'{g}'::geometry" for g in geometries)}]
                    )
                )
                WHERE id = $1;
                """

            # Execute the update query, passing the datastream_id
            await conn.execute(query, ds)


async def generate_observations(conn, commit_id):
    """
    Generates observations and inserts them into the database.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """

    observations = []
    for j in range(1, n_things * n_observed_properties + 1):
        phenomenonTime = date
        check_date = date

        while phenomenonTime < (date + interval):
            phenomenonTime += frequency
            phenomenonTimeRange = Range(
                phenomenonTime,
                phenomenonTime,
                upper_inc=True,
            )
            resultNumber = random.randint(1, 100)
            resultType = 0
            datastream_id = j
            featuresofinterest_id = random.randint(1, n_things)

            if commit_id is not None:
                observations.append(
                    (
                        phenomenonTimeRange,
                        resultNumber,
                        resultType,
                        datastream_id,
                        featuresofinterest_id,
                        commit_id,
                    )
                )
            else:
                observations.append(
                    (
                        phenomenonTimeRange,
                        resultNumber,
                        resultType,
                        datastream_id,
                        featuresofinterest_id,
                    )
                )
            if phenomenonTime >= (check_date + chunk):
                await insert_observations(conn, observations, commit_id)
                await update_datastream_phenomenon_time(
                    conn, observations, datastream_id
                )
                check_date = phenomenonTime
                observations = []

    if observations:
        await insert_observations(conn, observations, commit_id)
        await update_datastream_phenomenon_time(
            conn, observations, datastream_id
        )

    await update_datastream_observed_area(conn)


async def create_data():
    """
    Generates dummy data and inserts it into the database.

    This function connects to the database using the provided connection URL,
    generates various types of dummy data, and inserts them into the respective
    tables in the database. If any error occurs during the data generation or
    insertion process, the changes are rolled back and an error message is printed.

    After the creation is complete, the database connection is closed.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            try:
                user_id = None
                user_uri = "anonymous"
                if authorization:
                    user = await get_user(conn)
                    user_id = user["id"]
                    user_uri = user["uri"]

                commit_id = None
                if versioning or authorization:
                    commit_id = await generate_commit(conn, user_id, user_uri)

                if network:
                    network_ids = await generate_networks(conn, commit_id)
                else:
                    network_ids = []

                await generate_things(conn, commit_id)
                await generate_locations(conn, commit_id)
                await generate_things_locations(conn)
                await generate_historicallocations(conn, commit_id)
                await generate_locations_historicallocations(conn)
                await generate_observedProperties(conn, commit_id)
                await generate_sensors(conn, commit_id)
                await generate_datastreams(conn, commit_id, network_ids)
                await generate_featuresofinterest(conn, commit_id)
                await generate_observations(conn, commit_id)
            except Exception as e:
                print(f"An error occured: {e}")
    finally:
        await pool.close()


async def delete_data():
    """
    Deletes all data from the sensorthings tables in the database.

    This function connects to the database using the provided connection URL,
    and then deletes all records from the following tables:
    - "Thing"
    - "Location"
    - "Thing_Location"
    - "HistoricalLocation"
    - "Location_HistoricalLocation"
    - "ObservedProperty"
    - "Sensor"
    - "Datastream"
    - "FeaturesOfInterest"
    - "Observation"

    If any error occurs during the deletion process, the changes are rolled back
    and an error message is printed.

    After the deletion is complete, the database connection is closed.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            try:
                if authorization:
                    await conn.execute('DELETE FROM sensorthings."User"')
                if versioning or authorization:
                    await conn.execute('DELETE FROM sensorthings."Commit"')

                await conn.execute('DELETE FROM sensorthings."Thing"')
                await conn.execute('DELETE FROM sensorthings."Location"')
                await conn.execute('DELETE FROM sensorthings."Thing_Location"')
                await conn.execute(
                    'DELETE FROM sensorthings."HistoricalLocation"'
                )
                await conn.execute(
                    'DELETE FROM sensorthings."Location_HistoricalLocation"'
                )
                await conn.execute(
                    'DELETE FROM sensorthings."ObservedProperty"'
                )
                await conn.execute('DELETE FROM sensorthings."Sensor"')
                await conn.execute('DELETE FROM sensorthings."Datastream"')
                await conn.execute(
                    'DELETE FROM sensorthings."FeaturesOfInterest"'
                )
                await conn.execute('DELETE FROM sensorthings."Observation"')
            except Exception as e:
                print(f"An error occurred: {e}")
    finally:
        await pool.close()


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def _paginate_all(
    session: aiohttp.ClientSession,
    initial_url: str,
    label: str,
) -> list[dict]:
    """Walk @iot.nextLink until exhausted, collecting all raw items."""
    results: list[dict] = []
    url: Optional[str] = initial_url
    page = 0
    while url:
        payload = await _fetch_json(session, url)
        items = payload.get("value", [])
        results.extend(items)
        page += 1
        url = payload.get("@iot.nextLink") or None
        print(f"[migration] {label}: page {page}, {len(results)} total")
    return results


async def _fetch_frost(config_url: str, page_size: int, timeout: float) -> list[dict]:
    """
    Fetch all Things from FROST with inline Locations, Datastreams,
    Sensors, and ObservedProperties expanded.
    """
    http_timeout = aiohttp.ClientTimeout(total=timeout)
    expand = "Locations,Datastreams($expand=ObservedProperty,Sensor)"
    initial_url = f"{config_url}/Things?$expand={expand}&$top={page_size}"
    async with aiohttp.ClientSession(timeout=http_timeout) as session:
        return await _paginate_all(session, initial_url, "Things")


def _extract_geometry_wkt(location_obj: Optional[dict], epsg_code: int) -> Optional[str]:
    """
    Convert a GeoJSON geometry dict from FROST into a PostGIS WKT string.
    Returns None if geometry is absent or unrecognised.
    """
    if not location_obj:
        return None
    geom_type = location_obj.get("type", "")
    coords = location_obj.get("coordinates")
    if not coords:
        return None
    if geom_type == "Point":
        return f"SRID={epsg_code};POINT({coords[0]} {coords[1]})"
    if geom_type == "Polygon":
        ring = coords[0]
        points = ", ".join(f"{x} {y}" for x, y in ring)
        return f"SRID={epsg_code};POLYGON(({points}))"
    if geom_type == "MultiPoint":
        points = ", ".join(f"({x} {y})" for x, y in coords)
        return f"SRID={epsg_code};MULTIPOINT({points})"
    return None


async def _insert_sensor(conn, raw: dict, commit_id: Optional[int]) -> int:
    name = raw.get("name") or f"sensor_{raw['@iot.id']}"
    description = raw.get("description") or ""
    encoding_type = raw.get("encodingType") or "application/pdf"
    metadata = raw.get("metadata") or ""
    if isinstance(metadata, dict):
        metadata = json.dumps(metadata)

    if commit_id is not None:
        return await conn.fetchval(
            """
            INSERT INTO sensorthings."Sensor" (description, name, "encodingType", metadata, commit_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            description, name, encoding_type, metadata, commit_id,
        )
    return await conn.fetchval(
        """
        INSERT INTO sensorthings."Sensor" (description, name, "encodingType", metadata)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        description, name, encoding_type, metadata,
    )


async def _insert_observed_property(conn, raw: dict, commit_id: Optional[int]) -> int:
    name = raw.get("name") or f"op_{raw['@iot.id']}"
    definition = raw.get("definition") or "{}"
    if isinstance(definition, dict):
        definition = json.dumps(definition)
    description = raw.get("description") or ""

    if commit_id is not None:
        return await conn.fetchval(
            """
            INSERT INTO sensorthings."ObservedProperty" (name, definition, description, commit_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            name, definition, description, commit_id,
        )
    return await conn.fetchval(
        """
        INSERT INTO sensorthings."ObservedProperty" (name, definition, description)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        name, definition, description,
    )


async def _insert_location(conn, raw: dict, commit_id: Optional[int], epsg_code: int) -> Optional[int]:
    name = raw.get("name") or f"location_{raw['@iot.id']}"
    description = raw.get("description") or ""
    encoding_type = raw.get("encodingType") or "application/geo+json"
    wkt = _extract_geometry_wkt(raw.get("location"), epsg_code)
    if not wkt:
        print(f"[migration] skipping location {raw['@iot.id']}: no parseable geometry")
        return None

    if commit_id is not None:
        return await conn.fetchval(
            """
            INSERT INTO sensorthings."Location" (description, name, location, "encodingType", commit_id)
            VALUES ($1, $2, $3::public.geometry, $4, $5)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            description, name, wkt, encoding_type, commit_id,
        )
    return await conn.fetchval(
        """
        INSERT INTO sensorthings."Location" (description, name, location, "encodingType")
        VALUES ($1, $2, $3::public.geometry, $4)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        description, name, wkt, encoding_type,
    )


async def _insert_thing(conn, raw: dict, commit_id: Optional[int]) -> int:
    name = raw.get("name") or f"thing_{raw['@iot.id']}"
    description = raw.get("description") or ""
    properties = json.dumps(raw["properties"]) if raw.get("properties") else json.dumps({})

    if commit_id is not None:
        return await conn.fetchval(
            """
            INSERT INTO sensorthings."Thing" (description, name, properties, commit_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            description, name, properties, commit_id,
        )
    return await conn.fetchval(
        """
        INSERT INTO sensorthings."Thing" (description, name, properties)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        description, name, properties,
    )


def _parse_frost_phenomenon_time(raw_value: Optional[str]) -> Optional[Range]:
    """
    Parse a FROST phenomenonTime string into an asyncpg Range for direct
    insertion into the tstzrange column sensorthings."Datastream"."phenomenonTime".

    FROST sends this as an ISO 8601 interval string in "start/end" form
    (e.g. "2020-01-01T00:00:00.000Z/2020-06-01T00:00:00.000Z"). An
    open-ended interval uses ".." for the missing side. Returns None
    when raw_value is missing or either bound fails to parse, so the
    caller can simply omit the column rather than insert a bad range.
    """
    if not raw_value:
        return None

    parts = raw_value.split("/", 1)
    start_str = parts[0].strip()
    end_str = parts[1].strip() if len(parts) > 1 else ""

    try:
        start = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str and start_str != ".." else None
        end = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str and end_str != ".." else None
    except ValueError:
        print(f"[migration] could not parse phenomenonTime {raw_value!r}, leaving column unset")
        return None

    if start is None:
        return None

    return Range(start, end, upper_inc=False)


async def _insert_datastream(
    conn,
    raw: dict,
    istsos_thing_id: int,
    istsos_sensor_id: int,
    istsos_op_id: int,
    commit_id: Optional[int],
    network_id: int,
) -> int:
    name = raw.get("name") or f"datastream_{raw['@iot.id']}"
    description = raw.get("description") or ""
    observation_type = raw.get("observationType") or "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    uom = raw.get("unitOfMeasurement") or {"name": "", "symbol": "", "definition": ""}
    properties = json.dumps(raw["properties"]) if raw.get("properties") else None
    phenomenon_time = _parse_frost_phenomenon_time(raw.get("phenomenonTime"))
    result_time = raw.get("resultTime") or None

    cols = [
        '"name"', '"description"', '"unitOfMeasurement"', '"observationType"',
        '"thing_id"', '"sensor_id"', '"observedproperty_id"', '"network_id"',
    ]
    vals: list[Any] = [
        name, description, json.dumps(uom), observation_type,
        istsos_thing_id, istsos_sensor_id, istsos_op_id, network_id,
    ]

    if properties is not None:
        cols.append('"properties"')
        vals.append(properties)

    if commit_id is not None:
        cols.append('"commit_id"')
        vals.append(commit_id)

    if phenomenon_time is not None:
        cols.append('"phenomenonTime"')
        vals.append(phenomenon_time)

    if result_time is not None:
        cols.append('"resultTime"')
        vals.append(result_time)

    placeholders = ", ".join(f"${i+1}" for i in range(len(vals)))
    col_str = ", ".join(cols)

    return await conn.fetchval(
        f"""
        INSERT INTO sensorthings."Datastream" ({col_str}) VALUES ({placeholders})
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        *vals,
    )


async def migrate():
    """
    Fetch every Thing from FROST and insert the full entity tree into the
    local istSOS deployment via direct postgres writes.

    ID mapping dicts translate FROST @iot.id values to istSOS-assigned IDs
    so foreign key references stay consistent across all inserts.
    """
    print(f"[migration] starting -- source: {frost_base_url}")

    raw_things = await _fetch_frost(frost_base_url, frost_page_size, frost_timeout)
    print(f"[migration] fetched {len(raw_things)} Things from FROST")

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            user_id = None
            user_uri = "anonymous"
            if authorization:
                user = await get_user(conn)
                user_id = user["id"]
                user_uri = user["uri"]

            commit_id = None
            if versioning or authorization:
                commit_id = await conn.fetchval(
                    """
                    INSERT INTO sensorthings."Commit" (author, "encodingType", message, "actionType")
                    VALUES ($1, $2, $3, $4) RETURNING id
                    """,
                    user_uri, "text/plain", f"FROST migration from {frost_base_url}", "CREATE",
                ) if not authorization else await conn.fetchval(
                    """
                    INSERT INTO sensorthings."Commit" (author, "encodingType", message, "actionType", user_id)
                    VALUES ($1, $2, $3, $4, $5) RETURNING id
                    """,
                    user_uri, "text/plain", f"FROST migration from {frost_base_url}", "CREATE", user_id,
                )

            # Wipe any partial data from a previous migration attempt before starting fresh
            print("[migration] clearing any existing data before fresh insert")
            await conn.execute('DELETE FROM sensorthings."Observation"')
            await conn.execute('DELETE FROM sensorthings."Datastream"')
            await conn.execute('DELETE FROM sensorthings."FeaturesOfInterest"')
            await conn.execute('DELETE FROM sensorthings."Location_HistoricalLocation"')
            await conn.execute('DELETE FROM sensorthings."HistoricalLocation"')
            await conn.execute('DELETE FROM sensorthings."Thing_Location"')
            await conn.execute('DELETE FROM sensorthings."Thing"')
            await conn.execute('DELETE FROM sensorthings."Location"')
            await conn.execute('DELETE FROM sensorthings."Sensor"')
            await conn.execute('DELETE FROM sensorthings."ObservedProperty"')
            await conn.execute('DELETE FROM sensorthings."Network" WHERE name = $1', "frost-migration")
            print("[migration] database cleared, starting insert")

            # All migrated Datastreams are attributed to a single network representing
            # the FROST source being migrated from, since Datastream.network_id is NOT NULL.
            network_id = await conn.fetchval(
                """
                INSERT INTO sensorthings."Network" (name, commit_id)
                VALUES ($1, $2)
                RETURNING id
                """ if commit_id is not None else """
                INSERT INTO sensorthings."Network" (name)
                VALUES ($1)
                RETURNING id
                """,
                *(["frost-migration", commit_id] if commit_id is not None else ["frost-migration"]),
            )

            # frost_id -> istsos_id for deduplication across things sharing sensors/ops
            sensor_id_map: dict[int, int] = {}
            op_id_map: dict[int, int] = {}

            total_things = 0
            total_ds = 0

            for raw_thing in raw_things:
                frost_thing_id = raw_thing.get("@iot.id")
                if frost_thing_id is None:
                    continue

                istsos_thing_id = await _insert_thing(conn, raw_thing, commit_id)
                total_things += 1

                # Insert locations and link them to the thing
                for raw_loc in raw_thing.get("Locations", []):
                    frost_loc_id = raw_loc.get("@iot.id")
                    if frost_loc_id is None:
                        continue
                    istsos_loc_id = await _insert_location(conn, raw_loc, commit_id, epsg)
                    if istsos_loc_id is None:
                        continue

                    await conn.execute(
                        'INSERT INTO sensorthings."Thing_Location" (thing_id, location_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
                        istsos_thing_id, istsos_loc_id,
                    )

                    # One HistoricalLocation per location per thing
                    hl_id = await conn.fetchval(
                        """
                        INSERT INTO sensorthings."HistoricalLocation" (time, thing_id)
                        VALUES (NOW(), $1) RETURNING id
                        """ if commit_id is None else """
                        INSERT INTO sensorthings."HistoricalLocation" (time, thing_id, commit_id)
                        VALUES (NOW(), $1, $2) RETURNING id
                        """,
                        *([istsos_thing_id] if commit_id is None else [istsos_thing_id, commit_id]),
                    )
                    await conn.execute(
                        'INSERT INTO sensorthings."Location_HistoricalLocation" (location_id, historicallocation_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
                        istsos_loc_id, hl_id,
                    )

                # Insert datastreams with their sensor and observed property
                for raw_ds in raw_thing.get("Datastreams", []):
                    frost_ds_id = raw_ds.get("@iot.id")
                    if frost_ds_id is None:
                        continue

                    raw_sensor = raw_ds.get("Sensor") or {}
                    frost_sensor_id = raw_sensor.get("@iot.id")
                    if frost_sensor_id not in sensor_id_map:
                        if not raw_sensor.get("name"):
                            print(f"[migration] skipping datastream {frost_ds_id}: no Sensor data expanded")
                            continue
                        sensor_id_map[frost_sensor_id] = await _insert_sensor(conn, raw_sensor, commit_id)
                    istsos_sensor_id = sensor_id_map[frost_sensor_id]

                    raw_op = raw_ds.get("ObservedProperty") or {}
                    frost_op_id = raw_op.get("@iot.id")
                    if frost_op_id not in op_id_map:
                        if not raw_op.get("name"):
                            print(f"[migration] skipping datastream {frost_ds_id}: no ObservedProperty data expanded")
                            continue
                        op_id_map[frost_op_id] = await _insert_observed_property(conn, raw_op, commit_id)
                    istsos_op_id = op_id_map[frost_op_id]

                    await _insert_datastream(conn, raw_ds, istsos_thing_id, istsos_sensor_id, istsos_op_id, commit_id, network_id)
                    total_ds += 1

                if total_things % 100 == 0:
                    print(f"[migration] progress: {total_things} Things, {total_ds} Datastreams inserted")

            print(f"[migration] done -- {total_things} Things, {total_ds} Datastreams, "
                  f"{len(sensor_id_map)} Sensors, {len(op_id_map)} ObservedProperties inserted")
    finally:
        await pool.close()


if __name__ == "__main__":
    if run_migration:
        asyncio.run(migrate())
    elif delete_dummy_data:
        asyncio.run(delete_data())
    elif create_dummy_data:
        asyncio.run(create_data())