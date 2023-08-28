## Ceating and adding Synthetic data to the entity tables



The dummy_data directory contains the script for creating and populating the synthetic data. 



├── clear_data.py
├── config.yml
├── datastream.py
├── Dockerfile
├── featuresOfInterest.py
├── gen_data.py
├── historicalLocation.py
├── load_data.sh
├── location.py
├── loc.py
├── observation.py
├── observedProperty.py
├── postgres_data.py
├── requirements.txt
├── sensor.py
├── seq.py
├── test.py
└── thing.py



### Running the dummy_data script:

When docker compose file is run it will automatically create add the new synthetic data to the entity table of postgres database.

Adding or not adding data is based on the .env file variable ***dummy_data***.

When **dummy_data** variable is set to **True** it will run the scirpt to populate the data.

```
HOSTNAME=http://localhost:8018/v1.1/
dummy_data=True #True/False  When True database table will be cleared and populated with synthetic data
```



> note:  The script will first clear the data and then populate it.

If user wants to manually add data with out using the synthetic data, dummy_data variable should be set to **False**.

```
HOSTNAME=http://localhost:8018/v1.1/
dummy_data=False #True/False  When True database table will be cleared and populated with synthetic data
```



### Populating synthetic data to the database:

*gen_data.py*  calls the functions to create and populate data to all the entity tables as per *config.yaml* file. It first clears the table and adds the created data.



### Data creation: 

Files *datastream.py,  featuresOfInterest.py, historicalLocation.py, location.py, observation.py, observedProperty.py, sensor.py and  thing.py* creates dummy data for datastream,  featuresOfInterest, historicalLocation, location, observation, observedProperty, sensor and  thing entity table respectively and stores it in .csv file in the data folder of the parent directory.

It creates number of data as per the *config.yaml* file.

File loc.py is used to update the location.

seq.py is used to update counter of the primary key id, as primary key id, so that user can add data in the next row of entity. table 





### Adding data:

*postgres_data.py* file connects to the postgres database and pushes the csv files created to the database.

