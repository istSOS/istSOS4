# istSOS4

## Clone the istSOS4repository

```sh
git clone https://github.com/istSOS/istSOS4.git
```

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

## Security Configuration

When authentication is enabled, set these values in your environment:

- `AUTHORIZATION=1`
- `SECRET_KEY=<strong-random-value>`

Recommendations:

- Use a `SECRET_KEY` with at least 32 characters.
- Avoid placeholder values such as `secret`, `password`, or `changeme`.
- Generate the key using a secure source, for example:

```sh
openssl rand -hex 32
```

If `AUTHORIZATION=0`, the API starts with warning logs because authorization checks are disabled. Use this mode only in controlled development scenarios.

## Reference

For more information about the database and how populate it with synthetic data, refer to the [Database Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README.md)
