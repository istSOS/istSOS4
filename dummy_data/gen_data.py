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
from seq import alter_seq
import os
from id_num import id_sequence
# Assuming the config.yml file is in the current directory
config_file_path = "config.yml"
with open(config_file_path, 'r') as config_file:
    config_data = yaml.safe_load(config_file)

dummy_data_status = config_data['dummy_data']
print(dummy_data_status)
clear_data_status = config_data['clear_data']
print(clear_data_status)
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

start_datetime=config_data['start_datetime']
print(start_datetime)
timestep=config_data['timestep']
print(timestep)



def create_data():



    #pass function parameters to the module and create csv 
    # print(static_datastreams)

################static###################

#one thing,sensor, location, location history
#Datastream -10
#Observed property -10
#Observation - 1000
    #number of data values in each tables
    location_id,thing_id,historicalLoc_id,observedProperty_id,sensor_id,datastream_id,featuresOfInterest_id,observation_id=id_sequence()
    static_location =1
    static_thing=1
    static_historical_location=1
    static_sensor_data=1
    static_features_of_interest=1
    print("###### static data ########")
    generate_location_data(location_id+1,static_location) #start_id_num , number of location data
    generate_thing_data(thing_id+1,static_thing,static_location) #tart_id_num,thng num, loc number
    generate_historicalLocation_data(historicalLoc_id+1,static_historical_location,static_thing,static_location) # start_id_num, hist loc, thng num, loc number
    generate_observedProperty_data(observedProperty_id+1,static_observed_properties)
    generate_sensor_data(sensor_id+1,static_sensor_data)
    generate_datastream_data(datastream_id+1,static_datastreams,static_thing,static_sensor_data,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(featuresOfInterest_id+1,static_features_of_interest)
    generate_observation_data(observation_id+1,static_observations,datastream_id+1,static_datastreams,static_features_of_interest,start_datetime,timestep) #obs,data_stream_num,data_stream_num,feature_num

    print("__________Updating static data_____________")
    add_data()



################dynanmic###################

#one thing, sensor
#multiple location, location history - 500
#datasstream 8
#observed property 10
#observation 500

    location_id,thing_id,historicalLoc_id,observedProperty_id,sensor_id,datastream_id,featuresOfInterest_id,observation_id=id_sequence()

    dynamic_location =500
    dynamic_thing=1
    dynamic_historical_location=500
    dynamic_sensor_data=1
    dynamic_features_of_interest=500
    print("###### dynamic data ########")
    print("dynamic_data_is: ", location_id)
    generate_location_data(location_id+1,dynamic_location)
    # generate_thing_data(2,1,500) #thng num, loc number
    generate_thing_data(thing_id+1,dynamic_thing,dynamic_location)
    generate_historicalLocation_data(historicalLoc_id+1,dynamic_historical_location,dynamic_thing,dynamic_location) # hist loc, thng num, loc number
    generate_observedProperty_data(observedProperty_id+1,dynamic_observed_properties)
    generate_sensor_data(sensor_id+1,dynamic_sensor_data)
    generate_datastream_data(datastream_id+1,dynamic_datastreams,dynamic_thing,dynamic_sensor_data,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(featuresOfInterest_id+1,dynamic_features_of_interest)
    generate_observation_data(observation_id+1,dynamic_observations,datastream_id+1,dynamic_datastreams,dynamic_features_of_interest,start_datetime,timestep) #obs,data_stream_num,feature_num
    print("################observation id:",observation_id)
    print("__________Updating dynamic data_____________")
    add_data()

    print("updating locations")

 

    location_id,thing_id,historicalLoc_id,observedProperty_id,sensor_id,datastream_id,featuresOfInterest_id,observation_id=id_sequence()
    update_loc(location_id)
    print("################last..observation id:",observation_id)
    alter_seq(datastream_id+1,featuresOfInterest_id+1,historicalLoc_id+1,location_id+1,observation_id+1,observedProperty_id+1,sensor_id+1,thing_id+1)

# def alter_seq(Datastream,FeaturesOfInterest,HistoricalLocation,Location,Observation,ObservedProperty,Sensor,Thing):
if dummy_data_status == True and clear_data_status == False:
    # clear()
    create_data()
    print("data update successfull..")



    folder_path = 'data'

    # Get a list of all files in the folder
    file_list = os.listdir(folder_path)

    # Iterate through the files and delete CSV files
    for filename in file_list:
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            os.remove(file_path)
            print(f"Deleted: {filename}")    

elif dummy_data_status == True and clear_data_status == True:
    clear()
    create_data()
    print("data update successfull..")



    folder_path = 'data'

    # Get a list of all files in the folder
    file_list = os.listdir(folder_path)

    # Iterate through the files and delete CSV files
    for filename in file_list:
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            os.remove(file_path)
            print(f"Deleted: {filename}")  

elif dummy_data_status == False and clear_data_status == True:
    clear()

       
else:
    print("data retained")







