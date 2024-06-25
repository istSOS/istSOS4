import json
import os
import random
from datetime import datetime, time

import isodate
import psycopg2

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

connection_url = "dbname={db} user={user} password={password} host='database' port='5432'".format(
    db=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)


def generate_things(cur):
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
    VALUES (%s, %s, %s)
    """
    cur.executemany(insert_sql, things)


def generate_locations(cur):
    """
    Generate locations and insert them into the database.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """
    locations = []
    for i in range(1, n_things + 1):
        description = f"location {i}"
        name = f"location name {i}"
        location = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        encodingType = "application/pdf"
        locations.append((description, name, location, encodingType))

    insert_sql = """
    INSERT INTO sensorthings."Location" (description, name, location, "encodingType")
    VALUES (%s, %s, %s, %s)
    """
    cur.executemany(insert_sql, locations)


def generate_things_locations(cur):
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
    VALUES (%s, %s)
    """
    cur.executemany(insert_sql, things_locations)


def generate_historicallocations(cur):
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
    VALUES (%s, %s)
    """
    cur.executemany(insert_sql, historicallocations)


def generate_locations_historicallocations(cur):
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
    VALUES (%s, %s)
    """
    cur.executemany(insert_sql, locations_historicallocations)


def generate_observedProperties(cur):
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
    VALUES (%s, %s, %s)
    """
    cur.executemany(insert_sql, observedProperties)


def generate_sensors(cur):
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
    VALUES (%s, %s, %s, %s)
    """
    cur.executemany(insert_sql, sensors)


def generate_datastreams(cur):
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
            description = f"datastream {i}"
            name = f"datastream name {i}"
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
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, datastreams)


def generate_featuresofinterest(cur):
    """
    Generate features of interest and insert them into the database.

    Args:
        cur: The database cursor object.

    Returns:
        None
    """
    featuresofinterest = []
    for i in range(1, n_things + 1):
        description = f"featuresofinterest {i}"
        name = f"featuresofinterest name {i}"
        encodingType = "application/pdf"
        feature = "0101000020E6100000BA490C022B7F52C0355EBA490C624440"
        featuresofinterest.append((description, name, encodingType, feature))

    insert_sql = """
    INSERT INTO sensorthings."FeaturesOfInterest" (description, name, "encodingType", feature)
    VALUES (%s, %s, %s, %s)
    """
    cur.executemany(insert_sql, featuresofinterest)


def generate_observations(cur):
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
        while phenomenonTime < (date + interval):
            phenomenonTime += frequency
            resultInteger = random.randint(1, 100)
            resultType = 1
            datastream_id = j
            featuresofinterest_id = random.randint(1, n_things)
            observations.append(
                (
                    phenomenonTime,
                    resultInteger,
                    resultType,
                    datastream_id,
                    featuresofinterest_id,
                )
            )

    insert_sql = """
    INSERT INTO sensorthings."Observation" ("phenomenonTime", "resultInteger", "resultType", datastream_id, featuresofinterest_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, observations)


def create_data():
    """
    Generates dummy data and inserts it into the database.

    This function connects to the database using the provided connection URL,
    generates various types of dummy data, and inserts them into the respective
    tables in the database. If any error occurs during the data generation or
    insertion process, the changes are rolled back and an error message is printed.

    After the creation is complete, the database connection is closed.
    """
    conn = psycopg2.connect(connection_url)
    cur = conn.cursor()
    try:
        generate_things(cur)
        generate_locations(cur)
        generate_things_locations(cur)
        generate_historicallocations(cur)
        generate_locations_historicallocations(cur)
        generate_observedProperties(cur)
        generate_sensors(cur)
        generate_datastreams(cur)
        generate_featuresofinterest(cur)
        generate_observations(cur)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()


def delete_data():
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
    conn = psycopg2.connect(connection_url)
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM sensorthings."Thing"')
        cur.execute('DELETE FROM sensorthings."Location"')
        cur.execute('DELETE FROM sensorthings."Thing_Location"')
        cur.execute('DELETE FROM sensorthings."HistoricalLocation"')
        cur.execute('DELETE FROM sensorthings."Location_HistoricalLocation"')
        cur.execute('DELETE FROM sensorthings."ObservedProperty"')
        cur.execute('DELETE FROM sensorthings."Sensor"')
        cur.execute('DELETE FROM sensorthings."Datastream"')
        cur.execute('DELETE FROM sensorthings."FeaturesOfInterest"')
        cur.execute('DELETE FROM sensorthings."Observation"')
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    if delete_dummy_data:
        delete_data()
    if create_dummy_data:
        create_data()
