#adding data to 8 tables from csv files passing table name and file name as argument
import psycopg2
from psycopg2 import sql
import argparse
import csv


print("For help use: postgres_data.py -h")
argParser = argparse.ArgumentParser(description='Adding data to sensorthings database table using csv file ')

argParser.add_argument("-l", "--locationTable", help="path of csv file containing Location data")
argParser.add_argument("-t", "--thingTable", help="path of csv file containing Thing data")
argParser.add_argument("-hl", "--historicalLocationTable", help="path of csv file containing HistoricalLocation data")
argParser.add_argument("-op", "--observedPropertyTable", help="path of csv file containing ObservedProperty data")
argParser.add_argument("-s", "--sensorTable", help="path of csv file containing Sensor data")
argParser.add_argument("-d", "--dataStreamTable", help="path of csv file containing Datastream data")
argParser.add_argument("-f", "--featuresOfInterestTable", help="path of csv file containing FeaturesOfInterest data")
argParser.add_argument("-o", "--ObservationTable", help="path of csv file containing Observation data")


file_name = argParser.parse_args()
print("path of csv file provided for location table = %s" % file_name.locationTable)
print("path of csv file provided for thing table = %s" % file_name.thingTable)
print("path of csv file provided for thing table = %s" % file_name.historicalLocationTable)
print("path of csv file provided for thing table = %s" % file_name.observedPropertyTable)
print("path of csv file provided for thing table = %s" % file_name.sensorTable)
print("path of csv file provided for thing table = %s" % file_name.dataStreamTable)
print("path of csv file provided for thing table = %s" % file_name.featuresOfInterestTable)
print("path of csv file provided for thing table = %s" % file_name.ObservationTable)
print("__________________________________________________________________________________________")


conn = psycopg2.connect(
    host="172.17.0.1", 
    port="45432",
    database="istsos",
    user="admin",
    password="admin"
)



#adding data to table from csv
def add_data(table_name,file_paths):

   
    table_name = "\""+table_name+"\""
    schema_name = 'sensorthings'

    cursor = conn.cursor()

    with open(file_paths, 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Skip the header row

        for row in csv_data:
            insert_query = f"INSERT INTO {schema_name}.{table_name} VALUES ({','.join(['%s'] * len(row))})"
            cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()



table_name=["Location","Thing","HistoricalLocation","ObservedProperty","Sensor","Datastream","FeaturesOfInterest","Observation"]
file_paths = [file_name.locationTable,file_name.thingTable,file_name.historicalLocationTable,file_name.observedPropertyTable,file_name.sensorTable,file_name.dataStreamTable,file_name.featuresOfInterestTable,file_name.ObservationTable]

try:
    for i in range(0,8):
        if (file_paths[i]!= None):
            #print(file_paths[i])
            #add_data function call
            add_data(table_name[i],file_paths[i])
    print("Table updated successfully")
except Exception as error:
    print("Exception occured, Table not updated \n Error:")
    print(error)


