import random
import math

import psycopg2
from psycopg2 import sql






conn = psycopg2.connect(
    host="0.0.0.0", 
    port="45432",
    database="istsos",
    user="admin",
    password="admin"
)


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