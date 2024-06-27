# Quick start

## Clone the istSOSm repository

```sh
git clone -b traveltime https://github.com/istSOS/istsos-miu.git
```

## Start docker service

To start the Docker service, run:

```sh
docker compose -f dev_docker-compose.yml up -d
```

## Use Sensor Things APIs

Access the SensorThings API at: http://127.0.0.1:8018/istsos-miu/v1.1

## Reference

For more information about the database and how populate it with synthetic data, refer to the [Database Documentation](https://github.com/istSOS/istsos-miu/blob/traveltime/database/README.md)
