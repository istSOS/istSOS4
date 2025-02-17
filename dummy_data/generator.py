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

import asyncio
import json
import os
import random
from datetime import datetime, time

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
        name = f"thing name {i}"
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
        name = f"location name {i}"
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


async def generate_observedProperties(conn, commit_id):
    """
    Generate observed properties and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    observedProperties = []
    for i in range(1, n_observed_properties + 1):
        name = f"{random.choice(['Temperature', 'Humidity', 'Pressure', 'Light', 'CO2', 'Motion'])}_{i}"
        definition = f"http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/{name}"
        description = f"observedProperty {i}"
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
        name = f"sensor name {i}"
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


async def generate_datastreams(conn, commit_id):
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
            unitOfMeasurement = json.dumps(
                {
                    "name": "Centigrade",
                    "symbol": "C",
                    "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
                }
            )
            description = f"datastream {cnt}"
            name = f"datastream name {cnt}"
            observationType = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
            thing_id = i
            sensor_id = cnt
            observedproperty_id = j
            if commit_id is not None:
                datastreams.append(
                    (
                        unitOfMeasurement,
                        description,
                        name,
                        observationType,
                        thing_id,
                        sensor_id,
                        observedproperty_id,
                        commit_id,
                    )
                )
            else:
                datastreams.append(
                    (
                        unitOfMeasurement,
                        description,
                        name,
                        observationType,
                        thing_id,
                        sensor_id,
                        observedproperty_id,
                    )
                )
            cnt += 1

    if commit_id is not None:
        insert_sql = """
            INSERT INTO sensorthings."Datastream" ("unitOfMeasurement", description, name, "observationType", thing_id, sensor_id, observedproperty_id, commit_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
    else:
        insert_sql = """
            INSERT INTO sensorthings."Datastream" ("unitOfMeasurement", description, name, "observationType", thing_id, sensor_id, observedproperty_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
    await conn.executemany(insert_sql, datastreams)


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
        name = f"featuresofinterest name {i}"
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
            query = f"""
                UPDATE sensorthings."Datastream"
                SET "observedArea" = ST_ConvexHull(
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
            resultType = 1
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

                await generate_things(conn, commit_id)
                await generate_locations(conn, commit_id)
                await generate_things_locations(conn)
                await generate_historicallocations(conn, commit_id)
                await generate_locations_historicallocations(conn)
                await generate_observedProperties(conn, commit_id)
                await generate_sensors(conn, commit_id)
                await generate_datastreams(conn, commit_id)
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


if __name__ == "__main__":
    if delete_dummy_data:
        asyncio.run(delete_data())
    if create_dummy_data:
        asyncio.run(create_data())
