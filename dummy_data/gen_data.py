#Thing, sensor
#location, historicallocation, featureofinterest
#Datastream
#observedproperty
#observation



import yaml

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
from test import update_loc






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
    print("###### static data ########")
    generate_location_data(1,1) #start_id_num , number of location data
    generate_thing_data(1,1,1) #tart_id_num,thng num, loc number
    generate_historicalLocation_data(1,1,1,1) # start_id_num, hist loc, thng num, loc number
    generate_observedProperty_data(1,static_observed_properties)
    generate_sensor_data(1,1)
    generate_datastream_data(1,static_datastreams,1,1,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(1,1)
    generate_observation_data(1,static_observations,static_datastreams,1) #obs,data_stream_num,feature_num

    print("__________Updating static data_____________")
    add_data()



################dynanmic###################

#one thing, sensor
#multiple location, location history - 500
#datasstream 8
#observed property 10
#observation 500
    print("###### dynamic data ########")
    generate_location_data(2,500)
    generate_thing_data(2,1,500) #thng num, loc number
    generate_historicalLocation_data(2,500,1,500) # hist loc, thng num, loc number
    generate_observedProperty_data(static_observed_properties+1,dynamic_observed_properties)
    generate_sensor_data(2,1)
    generate_datastream_data(static_datastreams+1,dynamic_datastreams,1,1,static_observed_properties)  # datastream  num, thing num, sensor num, obs prop num
    generate_featuresOfInterest_data(2,500)
   
    generate_observation_data(static_observations+1,dynamic_observations,static_datastreams,1) #obs,data_stream_num,feature_num

    print("__________Updating dynamic data_____________")
    add_data()

    print("updatin g locations")

    update_loc()




create_data()







