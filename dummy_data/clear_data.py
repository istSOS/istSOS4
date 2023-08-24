import random
import math

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Specify the path to the .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# Load the environment variables from the .env file
load_dotenv(dotenv_path)

# Access environment variables
pg_host = os.getenv('POSTGRES_HOST')
pg_port = os.getenv('POSTGRES_PORT')

# print(f"postgres host: {pg_host}")
# print(f"postgres port: {pg_port}")





# conn = psycopg2.connect(
#     host=pg_host, 
#     port=pg_port,
#     database="istsos",
#     user="admin",
#     password="admin"
# )

conn = psycopg2.connect(
    host="172.17.0.1", 
    port="45432",
    database="istsos",
    user="admin",
    password="admin"
)


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

clear()





