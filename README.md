# istSOS4

## Clone the istSOS4repository

```sh
git clone -b traveltime https://github.com/istSOS/istSOS4.git
```

## Start DEV environment

To start the Docker services, run:

```sh
docker compose -f dev_docker-compose.yml up -d
```

To switch off the services:

```sh
docker compose -f dev-docker-compose.yml down
```

## Use Sensor Things APIs

Access the SensorThings API at: http://127.0.0.1:8018/istsos4/v1.1

## Reference

For more information about the database and how populate it with synthetic data, refer to the [Database Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README.md)
