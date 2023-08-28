#Thing, sensor
#location, historicallocation, featureofinterest
#Datastream
#observedproperty
#observation


import time
import yaml
from clear_data import clear
from thing import generate_thing_data
from sensor import generate_sensor_data
from location import generate_location_data
from historicalLocation import generate_historicalLocation_data
from featuresOfInterest import generate_featuresOfInterest_data
from datastream import generate_datastream_data
from observedProperty import generate_observedProperty_data
from observation import generate_observation_data
from postgres_data import add_data
import time
from loc import update_loc
import psycopg2

from seq import alter_seq

import os
from dotenv import load_dotenv

# Specify the path to the .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# Load the environment variables from the .env file
load_dotenv(dotenv_path)

# Access environment variables
dummy_data_status = os.getenv('dummy_data')





def create_data():



        # Assuming the config.yml file is in the current directory
    config_file_path = "config.yml"
    with open(config_file_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)

    # Access the config data
    # print(config_data)
    #static
    static_observed_properties=config_data['static_datastreams']['observedProperties']
    static_datastreams=config_data['static_datastreams']['quantity']
    static_observations=config_data['static_datastreams']['observations_each']    
    #dynamic
    dynamic_observed_properties=config_data['dynamic_datastreams']['observedProperties']
    dynamic_datastreams=config_data['dynamic_datastreams']['quantity']
    dynamic_observations=config_data['dynamic_datastreams']['observations_each']
    #pass function parameters to the module and create csv 
    # print(static_datastreams)

################static###################

#one thing,sensor, location, location history
#Datastream -10
#Observed property -10
#Observation - 1000
    #number of data values in each tables
    static_location =1
    static_thing=1
    static_historical_location=1
    static_sensor_data=1
    static_features_of_interest=1
    print("###### static data ########")
    generate_location_data(1,static_location) #start_id_num , number of location data
    generate_thing_data(1,static_thing,static_location) #tart_id_num,thng num, loc number
    generate_historicalLocation_data(1,static_historical_location,static_thing,static_location) # start_id_num, hist loc, thng num, loc number
    generate_observedProperty_data(1,static_observed_properties)
    generate_sensor_data(1,static_sensor_data)
    generate_datastream_data(1,static_datastreams,static_thing,static_sensor_data,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(1,static_features_of_interest)
    generate_observation_data(1,static_observations,static_datastreams,static_features_of_interest) #obs,data_stream_num,feature_num

    print("__________Updating static data_____________")
    add_data()



################dynanmic###################

#one thing, sensor
#multiple location, location history - 500
#datasstream 8
#observed property 10
#observation 500

    dynamic_location =500
    dynamic_thing=1
    dynamic_historical_location=500
    dynamic_sensor_data=1
    dynamic_features_of_interest=500
    print("###### dynamic data ########")
    generate_location_data(static_location+1,dynamic_location)
    # generate_thing_data(2,1,500) #thng num, loc number
    generate_thing_data(static_thing+1,dynamic_thing,dynamic_location)
    generate_historicalLocation_data(static_historical_location+1,dynamic_historical_location,dynamic_thing,dynamic_location) # hist loc, thng num, loc number
    generate_observedProperty_data(static_observed_properties+1,dynamic_observed_properties)
    generate_sensor_data(static_sensor_data+1,dynamic_sensor_data)
    generate_datastream_data(static_datastreams+1,dynamic_datastreams,dynamic_thing,dynamic_sensor_data,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(static_features_of_interest+1,dynamic_features_of_interest)
   
    generate_observation_data(static_observations*static_datastreams+1,dynamic_observations,static_datastreams,dynamic_features_of_interest) #obs,data_stream_num,feature_num

    print("__________Updating dynamic data_____________")
    add_data()

    print("updating locations")

    update_loc()
    alter_seq(static_datastreams+dynamic_datastreams+1,static_features_of_interest+dynamic_features_of_interest+1,static_historical_location+dynamic_historical_location+1,static_location+dynamic_location+1,static_observations*static_datastreams+dynamic_observations*dynamic_datastreams+1,static_observed_properties+dynamic_observed_properties+1,static_sensor_data+dynamic_sensor_data+1,static_thing+dynamic_thing+1)

# def alter_seq(Datastream,FeaturesOfInterest,HistoricalLocation,Location,Observation,ObservedProperty,Sensor,Thing):
if dummy_data_status=="True":
    clear()
    create_data()
    print("data update successfull..")

# try:
#     clear()  # Assuming this function is defined
#     create_data()  # Assuming this function is defined
# except psycopg2.OperationalError as e:
#     print("PostgreSQL connection issue:", e)
#     print("Trying to connect again...")
#     time.sleep(5)  # Add a delay before retrying
#     try:
#         clear()
#         create_data()
#     except Exception as e:
#         print("Error:", e)






