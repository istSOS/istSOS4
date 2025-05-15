import asyncio
import json
import os
import random
from datetime import datetime, time

from api.app.v1.endpoints.delete import EPSG, ST_Aggregate
import asyncpg
import isodate
from asyncpg.types import Range

create_dummy_data = int(os.getenv("DUMMY_DATA"))
delete_dummy_data = int(os.getenv("CLEAR_DATA"))
n_things = int(os.getenv("N_THINGS"))
n_observed_properties = int(os.getenv("N_OBSERVED_PROPERTIES"))
interval = isodate.parse_duration(os.getenv("INTERVAL"))
frequency = isodate.parse_duration(os.getenv("FREQUENCY"))
date = (
    datetime.strptime(os.getenv("START_DATETIME"), "%Y-%m-%dT%H:%M:%S.%f%z")
    if os.getenv("START_DATETIME")
    else datetime.combine(datetime.now().today(), time.min)
)
chunk = isodate.parse_duration(os.getenv("CHUNK_INTERVAL"))
epsg = int(os.getenv("EPSG"))
st_aggregate = (os.getenv("ST_AGGREGATE"))

pgpool = None


async def get_pool():
    """
    Retrieves or creates a connection pool to the PostgreSQL database.

    Returns:
        asyncpg.pool.Pool: The connection pool object.
    """
    global pgpool
    if not pgpool:
        pgpool = await asyncpg.create_pool(
            dsn=f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@database:5432/{os.getenv('POSTGRES_DB')}"
        )
    return pgpool


async def generate_things(conn):
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
        things.append((description, name, properties))

    insert_sql = """
    INSERT INTO sensorthings."Thing" (description, name, properties)
    VALUES ($1, $2, $3)
    """
    await conn.executemany(insert_sql, things)


async def generate_locations(conn):
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
        locations.append((description, name, location, encodingType))

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


async def generate_historicallocations(conn):
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
        historicallocations.append((time, thing_id))

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


async def generate_observedProperties(conn):
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
        observedProperties.append((name, definition, description))

    insert_sql = """
    INSERT INTO sensorthings."ObservedProperty" (name, definition, description)
    VALUES ($1, $2, $3)
    """
    await conn.executemany(insert_sql, observedProperties)


async def generate_sensors(conn):
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
        sensors.append((description, name, encodingType, metadata))

    insert_sql = """
    INSERT INTO sensorthings."Sensor" (description, name, "encodingType", metadata)
    VALUES ($1, $2, $3, $4)
    """
    await conn.executemany(insert_sql, sensors)


async def generate_datastreams(conn):
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

    insert_sql = """
    INSERT INTO sensorthings."Datastream" ("unitOfMeasurement", description, name, "observationType", thing_id, sensor_id, observedproperty_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    """
    await conn.executemany(insert_sql, datastreams)


async def generate_featuresofinterest(conn):
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
        featuresofinterest.append((description, name, encodingType, feature))

    insert_sql = """
    INSERT INTO sensorthings."FeaturesOfInterest" (description, name, "encodingType", feature)
    VALUES ($1, $2, $3, $4::public.geometry)
    """
    await conn.executemany(insert_sql, featuresofinterest)


async def insert_observations(conn, observations):
    await conn.copy_records_to_table(
        "Observation",
        records=observations,
        schema_name="sensorthings",
        columns=[
            "phenomenonTime",
            "resultNumber",
            "resultType",
            "datastream_id",
            "featuresofinterest_id",
        ],
    )


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
                SET "observedArea" = Set_SRID(ST_Extent(
                    ST_Collect(
                        ARRAY[{', '.join(f"'{g}'::geometry" for g in geometries)}]
                    )
                ), {EPSG} )
                WHERE id = $1;
                """

            # Execute the update query, passing the datastream_id
            await conn.execute(query, ds)


async def generate_observations(conn):
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
                await insert_observations(conn, observations)
                await update_datastream_phenomenon_time(
                    conn, observations, datastream_id
                )
                check_date = phenomenonTime
                observations = []

    if observations:
        await insert_observations(conn, observations)
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
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await generate_things(conn)
                await generate_locations(conn)
                await generate_things_locations(conn)
                await generate_historicallocations(conn)
                await generate_locations_historicallocations(conn)
                await generate_observedProperties(conn)
                await generate_sensors(conn)
                await generate_datastreams(conn)
                await generate_featuresofinterest(conn)
                await generate_observations(conn)
            except Exception as e:
                print(f"An error occured: {e}")


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
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
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
                await conn.close()


if __name__ == "__main__":
    if delete_dummy_data:
        asyncio.run(delete_data())
    if create_dummy_data:
        asyncio.run(create_data())
