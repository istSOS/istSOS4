import random
import math

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv






connection_url = "postgres://admin:admin@database:5432/istsos"
conn = psycopg2.connect(connection_url)


# conn = psycopg2.connect(
#     host=pg_host, 
#     port=pg_port,
#     database="istsos",
#     user="admin",
#     password="admin"
# )

# conn = psycopg2.connect(
#     host="172.17.0.1", 
#     port="45432",
#     database="istsos",
#     user="admin",
#     password="admin"
# )


def clear():

        

    # SQL statement for updating location geometry and updating id sequence number of 8 columns
    clear_table = f"""
        TRUNCATE TABLE  sensorthings."Observation" CASCADE;
        TRUNCATE TABLE  sensorthings."FeaturesOfInterest" CASCADE;
        TRUNCATE TABLE  sensorthings."Datastream" CASCADE;
        TRUNCATE TABLE  sensorthings."Sensor" CASCADE;
        TRUNCATE TABLE  sensorthings."ObservedProperty" CASCADE;
        TRUNCATE TABLE  sensorthings."HistoricalLocation" CASCADE;
        TRUNCATE TABLE  sensorthings."Thing" CASCADE;
        TRUNCATE TABLE  sensorthings."Location" CASCADE;

        
    """
    

    # Create a cursor
    cur = conn.cursor()

    # Execute the update statement
    cur.execute(clear_table)

    # Commit the changes
    conn.commit()
    cur.close()







