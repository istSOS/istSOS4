import random
import math

import psycopg2
from psycopg2 import sql






# conn = psycopg2.connect(
#     host="0.0.0.0", 
#     port="45432",
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
def update_loc():
    radius = 1000000                         #Choose your own radius
    radiusInDegrees=radius/111300            
    r = radiusInDegrees
    x0 = 21.311022205455732
    y0 = 79.43170395550221


    for i in range(1,500):

        u = float(random.uniform(0.0,1.0))
        v = float(random.uniform(0.0,1.0))

        w = r * math.sqrt(u)
        t = 2 * math.pi * v
        x = w * math.cos(t) 
        y = w * math.sin(t)

        xLat  = x + x0
        yLong = y + y0


        # print(str(xLat) + "," + str(yLong) + '\n')
        lat_long=str(xLat) + "," + str(yLong)
        




        # SQL statement for updating location geometry and updating id sequence number of 8 columns
        update_sql = f"""
            UPDATE sensorthings."Location"
            SET location = ST_SetSRID(ST_MakePoint({yLong}, {xLat}), 4326)
            WHERE id = {i};

            UPDATE sensorthings."FeaturesOfInterest"
            SET feature = ST_SetSRID(ST_MakePoint({yLong}, {xLat}), 4326)
            WHERE id = {i};


            
        """
    #  ALTER SEQUENCE sensorthings."Datastream_id_seq" RESTART WITH 1501;
        # update_sql = f"""
        #     UPDATE sensorthings."FeaturesOfInterest"
        #     SET feature = ST_SetSRID(ST_MakePoint({yLong}, {xLat}), 4326)
        #     WHERE id = {i};
        # """




        # Connect to the database
        # conn = psycopg2.connect(**db_params)

        # Create a cursor
        cur = conn.cursor()

        # Execute the update statement
        cur.execute(update_sql)

        # Commit the changes
        conn.commit()
        cur.close()




# update_loc()