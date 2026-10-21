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
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from time import perf_counter

import asyncpg
import isodate

hostname = os.getenv("HOSTNAME", "http://localhost:8018")
subpath = os.getenv("SUBPATH", "/istsos4")
version = os.getenv("VERSION", "/v1.1")
versioning = int(os.getenv("VERSIONING", "0"))
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
date = datetime.fromisoformat(
    os.getenv(
        "START_DATETIME", datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    )
)
chunk = isodate.parse_duration(os.getenv("CHUNK_INTERVAL", "P1Y"))
epsg = int(os.getenv("EPSG", 4326))
center_lat = float(os.getenv("CENTER_LAT", 45.8693))
center_lon = float(os.getenv("CENTER_LON", 8.9770))
spread_deg = float(os.getenv("SPREAD_DEG", 0.05))
authorization = int(os.getenv("AUTHORIZATION", 0))
st_aggregate = os.getenv("ST_AGGREGATE", "CONVEX_HULL")
batch_size = int(os.getenv("BATCH_SIZE", "10000"))
log_interval = float(os.getenv("LOG_INTERVAL_SECONDS", "5"))
LOG = logging.getLogger("dummy_data")

pgpool = None
network = int(os.getenv("NETWORK", 0))

observedProperties = []


def random_point():
    """
    Return a random (lon, lat) inside the configured box around the center.
    """

    return (
        random.uniform(center_lon - spread_deg, center_lon + spread_deg),
        random.uniform(center_lat - spread_deg, center_lat + spread_deg),
    )


async def get_pool():
    """
    Retrieves or creates a connection pool to the PostgreSQL database.

    Returns:
        asyncpg.pool.Pool: The connection pool object.
    """

    return await asyncpg.create_pool(
        host=pg_host, port=int(pg_write_port or pg_port), database=pg_db,
        user=pg_user, password=pg_password, min_size=1, max_size=1,
        server_settings={"application_name": "istsos4_dummy_loader"},
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

    rows = await conn.fetchmany(insert_sql + " RETURNING id", things)
    return [row["id"] for row in rows]


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
        lon, lat = random_point()
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
    rows = await conn.fetchmany(insert_sql + " RETURNING id", locations)
    return [row["id"] for row in rows]


async def generate_things_locations(conn, thing_ids, location_ids):
    """
    Generate and insert things locations into the database.

    Parameters:
    cur (cursor): The database cursor object.

    Returns:
    None
    """

    things_locations = []
    for thing_id, location_id in zip(thing_ids, location_ids):
        things_locations.append((thing_id, location_id))

    insert_sql = """
    INSERT INTO sensorthings."Thing_Location" (thing_id, location_id)
    VALUES ($1, $2)
    """
    await conn.executemany(insert_sql, things_locations)


async def generate_historicallocations(conn, commit_id, thing_ids):
    """
    Generate historical locations for things and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    historicallocations = []
    time = date
    for thing_id in thing_ids:
        time += frequency
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
    rows = await conn.fetchmany(insert_sql + " RETURNING id", historicallocations)
    return [row["id"] for row in rows]


async def generate_locations_historicallocations(conn, location_ids, historicallocation_ids):
    """
    Generate a list of tuples representing the relationship between location and historical location.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """

    locations_historicallocations = []
    for location_id, historicallocation_id in zip(location_ids, historicallocation_ids):
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
    observedProperties.clear()
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
    rows = await conn.fetchmany(insert_sql + " RETURNING id", observedProperties)
    return [row["id"] for row in rows]


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
    rows = await conn.fetchmany(insert_sql + " RETURNING id", sensors)
    return [row["id"] for row in rows]


async def generate_datastreams(conn, commit_id, network_ids, thing_ids, sensor_ids, observedproperty_ids):
    """
    Generate datastreams and insert them into the database.

    Args:
        cur (cursor): The database cursor.

    Returns:
        None
    """

    datastreams = []
    cnt = 1
    for thing_id in thing_ids:
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
                        "resolution": os.getenv("FREQUENCY", "PT5M"),
                        "qualityIndexLimits": {"max": "", "min": ""},
                        "acquisitionInterval": os.getenv("FREQUENCY", "PT5M"),
                    }
                ),
                "description": f"datastream {cnt}",
                "name": "{}_datastream_{}".format(
                    mapping_op[observedProperties[j - 1][0].rsplit("_", 1)[0]][0], cnt
                ),
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "thing_id": thing_id,
                "sensor_id": sensor_ids[cnt - 1],
                "observedproperty_id": observedproperty_ids[j - 1],
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
    rows = await conn.fetchmany(
        insert_sql + " RETURNING id", [tuple(ds.values()) for ds in datastreams]
    )
    return [row["id"] for row in rows]


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
        lon, lat = random_point()
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
    rows = await conn.fetchmany(insert_sql + " RETURNING id", featuresofinterest)
    return [row["id"] for row in rows]


def observations_per_stream():
    """Count the existing start-exclusive, end-inclusive sampling schedule."""
    end = date + interval
    if isinstance(frequency, timedelta):
        return -(-(end - date) // frequency)
    count = 0
    timestamp = date
    while timestamp < end:
        timestamp += frequency
        count += 1
    return count


def validate_config():
    if n_things <= 0 or n_observed_properties <= 0 or batch_size <= 0:
        raise ValueError("N_THINGS, N_OBSERVED_PROPERTIES and BATCH_SIZE must be positive")
    if date.tzinfo is None or date.utcoffset() is None:
        raise ValueError("START_DATETIME must include a timezone")
    if date + interval <= date or date + frequency <= date or date + chunk <= date:
        raise ValueError("INTERVAL, FREQUENCY and CHUNK_INTERVAL must be positive")
    if st_aggregate not in ("CONVEX_HULL", "ENVELOPE"):
        raise ValueError("ST_AGGREGATE must be CONVEX_HULL or ENVELOPE")
    if log_interval < 0:
        raise ValueError("LOG_INTERVAL_SECONDS must be nonnegative")


async def insert_observations(conn, observations, commit_id):
    columns = [
        "phenomenonTimeStart", "phenomenonTimeEnd", "resultTime",
        "resultNumber", "resultType", "datastream_id", "featuresofinterest_id",
    ]
    if commit_id is not None:
        columns.append("commit_id")
    await conn.copy_records_to_table(
        "Observation", schema_name="sensorthings", columns=columns, records=observations
    )


async def update_datastream_phenomenon_time(conn, observations, datastream_id):
    # Batches contain one stream in ascending timestamp order.
    await conn.execute(
        '''
        UPDATE sensorthings."Datastream"
        SET "phenomenonTime" = tstzrange(
                LEAST($1::timestamptz, lower("phenomenonTime")),
                GREATEST($2::timestamptz, upper("phenomenonTime")), '[]'),
            "resultTime" = tstzrange(
                LEAST($3::timestamptz, lower("resultTime")),
                GREATEST($4::timestamptz, upper("resultTime")), '[]'),
            last_foi_id = $5
        WHERE id = $6
        ''',
        observations[0][0], observations[-1][1],
        observations[0][2], observations[-1][2],
        observations[-1][6], datastream_id,
    )


async def update_datastream_observed_area(conn, datastream_id, feature_ids):
    # The generator already knows the distinct features used: never rescan observations.
    aggregate = "ST_ConvexHull" if st_aggregate == "CONVEX_HULL" else "ST_Envelope"
    await conn.execute(
        f'''
        UPDATE sensorthings."Datastream"
        SET "observedArea" = (
            SELECT {aggregate}(ST_Collect(feature))
            FROM sensorthings."FeaturesOfInterest" WHERE id = ANY($1::bigint[])
        )
        WHERE id = $2
        ''',
        sorted(feature_ids), datastream_id,
    )


async def generate_observations(conn, commit_id, datastream_ids, feature_ids):
    """COPY bounded batches, committing their derived fields atomically."""
    started = perf_counter()
    stats = dict(rows=0, batches=0, generation_seconds=0.0, copy_seconds=0.0,
                 derived_seconds=0.0, transaction_seconds=0.0)
    planned = observations_per_stream() * len(datastream_ids)
    end = date + interval
    last_log = started
    LOG.info("Loading %s observations in %s Datastreams; BATCH_SIZE=%s",
             f"{planned:,}", len(datastream_ids), batch_size)

    for datastream_id in datastream_ids:
        observations = []
        used_features = set()
        persisted_features = set()
        timestamp = date
        chunk_start = date
        generation_started = perf_counter()
        while timestamp < end:
            timestamp += frequency
            result = random.randint(1, 100)
            feature_id = random.choice(feature_ids)
            used_features.add(feature_id)
            row = (timestamp, timestamp, timestamp, result, 0, datastream_id, feature_id)
            if commit_id is not None:
                row += (commit_id,)
            observations.append(row)
            if (len(observations) < batch_size and timestamp < chunk_start + chunk
                    and timestamp < end):
                continue

            stats["generation_seconds"] += perf_counter() - generation_started
            transaction_started = perf_counter()
            async with conn.transaction():
                phase_started = perf_counter()
                await insert_observations(conn, observations, commit_id)
                copy_seconds = perf_counter() - phase_started
                phase_started = perf_counter()
                await update_datastream_phenomenon_time(conn, observations, datastream_id)
                if used_features != persisted_features:
                    await update_datastream_observed_area(conn, datastream_id, used_features)
                derived_seconds = perf_counter() - phase_started
            stats["copy_seconds"] += copy_seconds
            stats["derived_seconds"] += derived_seconds
            stats["transaction_seconds"] += perf_counter() - transaction_started - copy_seconds - derived_seconds
            stats["rows"] += len(observations)
            stats["batches"] += 1
            persisted_features = used_features.copy()
            now = perf_counter()
            if now - last_log >= log_interval or stats["rows"] == planned:
                LOG.info("Committed %s/%s observations (%.1f%%), %.0f rows/s, Datastream %s",
                         f'{stats["rows"]:,}', f"{planned:,}", 100 * stats["rows"] / planned,
                         stats["rows"] / (now - started), datastream_id)
                last_log = now
            observations = []
            chunk_start = timestamp
            generation_started = perf_counter()

    stats["seconds"] = perf_counter() - started
    LOG.info("Observation timings: generation=%.3fs COPY=%.3fs derived=%.3fs transaction=%.3fs",
             stats["generation_seconds"], stats["copy_seconds"],
             stats["derived_seconds"], stats["transaction_seconds"])
    return stats


async def create_data():
    """Commit metadata first, then observations in bounded transactions.

    Errors propagate: earlier batches remain committed, the failing batch rolls back.
    """
    validate_config()
    if os.getenv("RANDOM_SEED"):
        random.seed(int(os.environ["RANDOM_SEED"]))
    started = perf_counter()
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            if not await conn.fetchval("SELECT pg_try_advisory_lock(hashtext('istsos4_dummy_data'))"):
                raise RuntimeError("Another dummy-data loader is running")
            try:
                async with conn.transaction():
                    exists = await conn.fetchval(
                        'SELECT EXISTS(SELECT 1 FROM sensorthings."Thing" WHERE name = ANY($1::text[]))',
                        [f"thing_name_{i}" for i in range(1, n_things + 1)],
                    )
                    if exists:
                        raise ValueError("Dummy Things already exist; use a fresh database or explicitly CLEAR_DATA=1")
                    user_id, user_uri = None, "anonymous"
                    if authorization:
                        user = await get_user(conn)
                        if user is None:
                            raise ValueError("ISTSOS_ADMIN has no User record")
                        user_id, user_uri = user["id"], user["uri"]
                    commit_id = None
                    if versioning or authorization:
                        commit_id = await generate_commit(conn, user_id, user_uri)
                    network_ids = await generate_networks(conn, commit_id) if network else []
                    thing_ids = await generate_things(conn, commit_id)
                    location_ids = await generate_locations(conn, commit_id)
                    await generate_things_locations(conn, thing_ids, location_ids)
                    historical_ids = await generate_historicallocations(conn, commit_id, thing_ids)
                    await generate_locations_historicallocations(conn, location_ids, historical_ids)
                    property_ids = await generate_observedProperties(conn, commit_id)
                    sensor_ids = await generate_sensors(conn, commit_id)
                    stream_ids = await generate_datastreams(
                        conn, commit_id, network_ids, thing_ids, sensor_ids, property_ids
                    )
                    feature_ids = await generate_featuresofinterest(conn, commit_id)
                metadata_seconds = perf_counter() - started
                LOG.info("Metadata committed in %.3fs", metadata_seconds)
                stats = await generate_observations(conn, commit_id, stream_ids, feature_ids)
                analyze_started = perf_counter()
                await conn.execute('ANALYZE sensorthings."Observation"; ANALYZE sensorthings."Datastream"')
                stats["analyze_seconds"] = perf_counter() - analyze_started
                stats["metadata_seconds"] = metadata_seconds
                stats["total_seconds"] = perf_counter() - started
                LOG.info("Finished: %s observations in %.3fs (%.0f rows/s)",
                         f'{stats["rows"]:,}', stats["total_seconds"],
                         stats["rows"] / stats["total_seconds"])
                LOG.info("STATS %s", json.dumps(stats, sort_keys=True))
                return stats
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext('istsos4_dummy_data'))")
    finally:
        await pool.close()


async def delete_data():
    """Explicitly clear core data transactionally, preserving the configured User."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            if not await conn.fetchval("SELECT pg_try_advisory_lock(hashtext('istsos4_dummy_data'))"):
                raise RuntimeError("Another dummy-data loader is running")
            try:
                async with conn.transaction():
                    for table in (
                        "Observation", "Thing_Location", "Location_HistoricalLocation",
                        "HistoricalLocation", "Datastream", "Thing", "Location",
                        "ObservedProperty", "Sensor", "FeaturesOfInterest",
                    ):
                        await conn.execute(f'DELETE FROM sensorthings."{table}"')
                    if network:
                        await conn.execute('DELETE FROM sensorthings."Network"')
                    if versioning or authorization:
                        await conn.execute('DELETE FROM sensorthings."Commit"')
            finally:
                await conn.execute("SELECT pg_advisory_unlock(hashtext('istsos4_dummy_data'))")
    finally:
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if delete_dummy_data:
        asyncio.run(delete_data())
    if create_dummy_data:
        asyncio.run(create_data())
