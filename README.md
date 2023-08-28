# CREATE FASTAPI

```
sudo docker compose up -d
```
>For updating the docker image use the command: \
```sudo docker compose up -d --build```

## See the automatic interactive API documentation

http://127.0.0.1:8018/v1.1/docs

## Install httpx for test

```
pip install httpx
```

## Change IP address for test

Inside app/test_main.py file insert yours IP address at line 13:

pgpool = await asyncpg.create_pool(dsn='postgresql://admin:admin@<IP_ADDRESS>:55432/istsos_test')

## Run test

```
cd /app
pytest
```

TBD

# VERSIONING

docker compose up -d

if not exists load DB schema 'sensorthings' & functions from 
\database\istsos_schema.sql
## update schema in postgREST
docker compose kill -s SIGUSR1 postgrest

## See the automatic interactive API documentation

http://127.0.0.1:8018/v1.1/docs


## USE STA APIs (proof of concept)

http://127.0.0.1:8018/v1.1/Observations?as_of_system_time=2023-04-23T15:56:16.123000%2B02:00
http://127.0.0.1:8018/v1.1/Observations

http://localhost:3000/Thing?select=id,name,Location(*),Datastream(id,name,unitOfMeasurement,ObservedProperty(name),Observation(result,phenomenonTime))&limit=1000&properties-%3E%3Emodel=eq.TS-100&Datastream.Observation.order=phenomenonTime.desc&Datastream.Observation.limit=1

http://localhost:3000/Thing?
    select=
        id,
        name,
        Location(*),
        Datastream(
            id,
            name,
            unitOfMeasurement,
            ObservedProperty(
                name
            ),
            Observation(
                result,phenomenonTime
            )
        )
    &limit=1000
    &properties-%3E%3Emodel=eq.TS-100
    &Datastream.Observation.order=phenomenonTime.desc
    &Datastream.Observation.limit=1

/v1.1/Things?
    $select=id,name,description,properties
    &$top=1000
    &$filter=properties/type eq 'station'
    &$expand=
    Locations,
        Datastreams(
            $select=
                id,name,unitOfMeasurement
            ;$expand=
                ObservedProperty($select=name),
                Observations(
                    $select=result,phenomenonTime
                    ;$orderby=phenomenonTime desc
        ;$top=1
        )
    )

## Fast API connection to data base

Run the following commands:
`cd fastapi/app/v1`
`uvicorn get_datastream:app --reload`

In the browser type "IP:port"//datastreams/1

You can also check using : http://127.0.0.1:8000/docs

> Change the host IP address in the script as per your docker IP in get_datastream.py file.
    
## Adding dummy data to database

 When you build the docker the script will automatically clear the database and add the static and dynamic values as per config.yaml file. 

For disabling addtion of the synthetic data to database
inside the ```.env```

change the variable  **dummy_data** to **False**

```
HOSTNAME=http://localhost:8018/v1.1/
dummy_data=False #True/False  When True database table will be cleared and populated with synthetic data
```

<!-- 
You can also run the script once the docker is build

Inside ```dummy_data``` folder run the gen_data.py script

For populating data: </br>
```python3 gen_data.py```
> populating data will first clear all the intial data from the database table and then will add data as per config file

For clearing data: </br>
```python3 clear_data.py```
> This will clear all data from the tables

>
## importing hoppscotch files

 - open hoppscotch.io 
 - login
 - import json file from `API_test` folder
 > for CORS error download the browser plugin of hoppscotch </br>
 for more details refer [here](https://docs.hoppscotch.io/documentation/features/interceptor).
 
 