
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

#for changing incrementing sequence id
def alter_seq(Datastream,FeaturesOfInterest,HistoricalLocation,Location,Observation,ObservedProperty,Sensor,Thing):
 







        # Commit the changes

    update_sql = f"""
    ALTER SEQUENCE sensorthings."Datastream_id_seq" RESTART WITH {Datastream};
    ALTER SEQUENCE sensorthings."FeaturesOfInterest_id_seq" RESTART WITH {FeaturesOfInterest};
    ALTER SEQUENCE sensorthings."HistoricalLocation_id_seq" RESTART WITH {HistoricalLocation};
    ALTER SEQUENCE sensorthings."Location_id_seq" RESTART WITH {Location};
    ALTER SEQUENCE sensorthings."Observation_id_seq" RESTART WITH {Observation};
    ALTER SEQUENCE sensorthings."ObservedProperty_id_seq" RESTART WITH {ObservedProperty};
    ALTER SEQUENCE sensorthings."Sensor_id_seq" RESTART WITH {Sensor};
    ALTER SEQUENCE sensorthings."Thing_id_seq" RESTART WITH {Thing};
    
        """

    cur = conn.cursor()

    # Execute the update statement
    cur.execute(update_sql)
    
    # Close the cursor and connection
    conn.commit()
    cur.close()

    conn.close()

# update_loc()