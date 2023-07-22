# CREATE FASTAPI

```
docker-compose up -d
```
>For updating the docker image use the command: \
```docker-compose up -d --build```

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
    

## Adding dummy data to postgresql sta database tables

Run `postgres_data.py` script inside db folder of fastapi with argument as a csv file to each table
```
cd fastapi/app/db
```
for checking the argument parameters to pass
```
python3 postgres_data.py -h
```

Example: For adding location entity data in table:

```
python3 postgres_data.py -l Location.csv
```

Similarly  u can update the csv files and pass the arguments to update all the tables.


## Randomly populating data to data base
Script to generate random data as needed to each table in csv format

```fastapi/app/db```
- There are 8 scripts for each entity data
- Run "table-name".py file
eg. ```python3 location.py``` will create 200 unique datasets..abd similarly u can run other files to generate datasets

Script to upload datasets to table
Once datasets are ready u can upload the csv using the following script

- ```postgres_data.py``` script will take argument of the csv file and load in to the table.
- for checking argument u can run
```python3 postgres_data.py -t Thing.csv ```
- This will load the datasets

> Make sure ur table does have counter id of primary key not initialized



