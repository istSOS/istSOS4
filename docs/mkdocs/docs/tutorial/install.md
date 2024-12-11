# istSOS4 Installation
Installing istSOS4 is straightforward with Docker. Follow these steps to get started.

## Prerequisites
Before proceeding, ensure you have Docker installed on your machine. If not, download and install Docker from the [official Dokcer website](https://docs.docker.com/get-docker/).

For detailed installation instructions, refer to the [Docker documentation](https://docs.docker.com/get-docker/).

## Jupiter Notebook
To participate in the hands-on tutorial, make sure Jupyter Notebook is available on your system.

### Installing Jupyter Notebook
If Jupyter Notebook is not already installed, follow these platform-specific instructions:

- Linux User: Install Jupyter Notebook by following the guide on the [official Jupyter website](https://jupyter.org/install).
- Windows User: Use [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html) to install Jupyter Notebook.

### Using Docker
Alternatively, you can use a Docker image that comes with Jupyter Notebook pre-installed.

Run the following command to start the Docker container with Jupyter Notebook:

```sh
docker run -it --rm -p 10000:8888 \
-v "${PWD}":/home/jovyan/work quay.io/jupyter/datascience-notebook:latest \
-c start-notebook.py --IdentityProvider.token=''
```

## Using Preconfigured Setup

To get started, create a `docker-compose.yml` file on your machine and add the following content:


```yaml
services:
  database:
    image: ghcr.io/istsos/istsos4/database:1.1
    environment:
      POSTGRES_DB: istsos
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      DATADIR: /var/lib/postgresql/data
    command: >
      postgres
        -c custom.versioning=1
        -c custom.authorization=1
        -c custom.duplicates=0
        -c custom.epsg=4326
        -c custom.user=admin
        -c custom.password=admin
    healthcheck:
      test: pg_isready -U postgres -d postgres
      interval: 10s
      timeout: 5s
      retries: 5

  api:
    image: ghcr.io/istsos/istsos4/api:1.3
    environment:
      HOSTNAME: http://localhost:8018
      SUBPATH: /istsos4
      VERSION: /v1.1
      DEBUG: 0
      VERSIONING: 1
      POSTGRES_DB: istsos
      ISTSOS_ADMIN: admin
      ISTSOS_ADMIN_PASSWORD: admin
      POSTGRES_HOST: database
      PG_MAX_OVERFLOW: 0
      PG_POOL_SIZE: 10
      PG_POOL_TIMEOUT: 30
      COUNT_MODE: FULL
      COUNT_ESTIMATE_THRESHOLD: 10000
      TOP_VALUE: 100
      PARTITION_CHUNK: 10000
      REDIS: 1
      EPSG: 4326
      AUTHORIZATION: 1
      SECRET_KEY: 09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7
      ACCESS_TOKEN_EXPIRE_MINUTES: 240
      ALGORITHM: HS256
      ANONYMOUS_VIEWER: 0
    command: uvicorn --timeout-keep-alive 75 --workers 2 --host 0.0.0.0 --port 5000 app.main:app
    ports:
      - 8018:5000
    working_dir: /code

  redis:
    image: redis:7.4.0-alpine3.20
    restart: always
```

To start the services, run:

```sh
docker compose up -d
```

To switch off the services:

```sh
docker compose down
```

To remove all images and volumes:

```sh
docker compose down -v --rmi all
```

## Use Sensor Things APIs

Access the SensorThings API with the interactive docs at:

http://localhost:8018/istsos4/v1.1/docs
