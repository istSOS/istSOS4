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

Login endpoint protection is enabled by default and can be tuned with:

- `LOGIN_RATE_LIMIT_ENABLED` (default `1`)
- `LOGIN_MAX_ATTEMPTS` (default `5`)
- `LOGIN_WINDOW_SECONDS` (default `60`)
- `LOGIN_BLOCK_SECONDS` (default `300`)

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


## Start EDU environment for tutorial and learning

```sh
docker compose -f edu_docker-compose.yml up -d
```

To switch off the services:

```sh
docker compose -f edu_docker-compose.yml down
```

To remove all images and volumes:

```sh
docker compose -f edu_docker-compose.yml down -v --rmi local
```


## Use Sensor Things APIs

Access the SensorThings API at: http://127.0.0.1:8018/istsos4/v1.1

## Reference

For more information about the database and how populate it with synthetic data, refer to the [Database Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README.md)
