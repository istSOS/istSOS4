# CREATE FASTAPI

```
docker-compose up -d
```

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