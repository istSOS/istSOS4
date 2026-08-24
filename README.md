# istSOS4

## Clone the istSOS4repository

```sh
git clone https://github.com/istSOS/istSOS4.git
```

## Environment setup

Before starting any environment, copy the example env file and fill in your values:

```sh
cp .env.example .env
```

At minimum set `SECRET_KEY` (required for authentication) and the Postgres/admin
passwords before running the stack.  All other variables default to sensible
development values.

## Start DEV environment

To start the Docker services, run:

```sh
docker compose -f dev_docker-compose.yml up -d
```

To switch off the services:

```sh
docker compose -f dev_docker-compose.yml down
```

To remove all images and volumes:

```sh
docker compose -f dev_docker-compose.yml down -v --rmi local
```


## Restore database from backup

```sh
docker exec -i istsos4-database sh -c '
  PGPASSWORD="$POSTGRES_PASSWORD" \
  pg_restore \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v \
    --clean \
    --if-exists
' < istsos4.backup
```

## Use Sensor Things APIs

Access the SensorThings API at: http://127.0.0.1:8018/istsos4/v1.1

## Reference

For more information about the database and how populate it with synthetic data, refer to the [Database Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README.md)
