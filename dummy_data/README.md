## Ceating and adding Synthetic data to the entity tables



The dummy_data directory contains the script for creating and populating the synthetic data. 

Following is the file structure:
```

├── clear_data.py
├── config.yml
├── data
├── datastream.py
├── Dockerfile
├── featuresOfInterest.py
├── gen_data.py
├── historicalLocation.py
├── id_num.py
├── load_data.sh
├── location.py
├── loc.py
├── observation.py
├── observedProperty.py
├── postgres_data.py
├── README.md
├── requirements.txt
├── sensor.py
├── seq.py
└── thing.py

```
### Configuration setup

For updating the database with sythetically generated database the following `config.yaml` file should be updated with status and count. 

```
# config.yml content
version: 2.1


dummy_data : True #True/False  When True database table will be cleared and populated with synthetic data
clear_data : True #True/False  True: clearing database False: Not clearing

# dummy_data, clear_data | Table Data Status
# _______________________|____________________________________
# True      ,True        | Previous data cleared and new data added
# True      ,False       | Dummy_data added over previous data
# False     ,True        | All data cleared 
# False     ,False       | Previous data retained

start_datetime: 2020-01-01T12:00:00.000+01:00
timestep: PT10M

static_datastreams: 
  observedProperties: 10
  quantity: 10
  observations_each: 1000

dynamic_datastreams: 
  observedProperties: 10
  quantity: 8
  observations_each: 500

```

The data will be populated as per the status of dummy_data and clear_data in the config file.
Change the values in config file as per need.
Truth table of the dummy_data and clear_data is mentioned in the commented lines of config file as shown above 





### Populating synthetic data to the database:

*gen_data.py*  calls the functions to create and populate data to all the entity tables as per *config.yaml* file. It first clears the table and adds the created data.



### Data creation: 

Files *datastream.py,  featuresOfInterest.py, historicalLocation.py, location.py, observation.py, observedProperty.py, sensor.py and  thing.py* creates dummy data for datastream,  featuresOfInterest, historicalLocation, location, observation, observedProperty, sensor and  thing entity table respectively and stores it in .csv file in the data folder of the parent directory.

It creates number of data as per the *config.yaml* file.

File loc.py is used to update the location.

seq.py is used to update counter of the primary key id, as primary key id, so that user can add data in the next row of entity. table 

File id_num.py will update the id of the table



### Adding data:

*postgres_data.py* file connects to the postgres database and pushes the csv files created to the database.

