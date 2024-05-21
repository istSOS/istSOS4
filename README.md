# Quick start

## Start docker service

```
docker compose up -d
```

## Use Sensor Things APIs

http://127.0.0.1:8018/istsos-miu/v1.1

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
 
 
