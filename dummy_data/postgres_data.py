#adding data to 8 tables from csv files passing table name and file name as argument
import psycopg2
from psycopg2 import sql
import csv

import os





connection_url = "postgres://admin:admin@database:5432/istsos"
conn = psycopg2.connect(connection_url)


            
#adding data to table from csv
def add_data():

    table_name=["Location","Thing","HistoricalLocation","ObservedProperty","Sensor","Datastream","FeaturesOfInterest","Observation"]
    file_paths = ["data/Location.csv","data/Thing.csv","data/HistoricalLocation.csv","data/ObservedProperty.csv","data/Sensor.csv","data/Datastream.csv","data/FeaturesOfInterest.csv","data/Observation.csv"]

    try:
        for i in range(0,8):

            #print(file_paths[i])
            #add_data function call
            # add_data(table_name[i],file_paths[i])

            table = "\""+table_name[i]+"\""
            schema_name = 'sensorthings'
            print(table)

            cursor = conn.cursor()

            with open(file_paths[i], 'r') as file:
                csv_data = csv.reader(file)
                next(csv_data)  # Skip the header row

                for row in csv_data:
                    #insert_query = f"INSERT INTO {schema_name}.{table_name} VALUES ({','.join(['%s'] * len(row))})"
                    
                    #print([ type(z) for z in row ])
                    insert_query = f"""INSERT INTO {schema_name}.{table} VALUES ({','.join([ "'"+str(z)+"'" if str(z)!="None" else 'null' for z in row ] )})"""
                    
                    #insert_query = f"""INSERT INTO {schema_name}.{table_name} VALUES (6,'2023-03-25 14:30:00+00','2023-03-25 14:30:00+00',23.5,null,null,null,1,1)"""
                    #print(insert_query)
                    cursor.execute(insert_query,row)




            conn.commit()
            cursor.close()
        # conn.close()
 
 
 
        print("Table updated successfully")
        print("_____________________________________________________")
    except Exception as error:
        print("Exception occured, Table not updated \n Error:")
        print(error)

   






