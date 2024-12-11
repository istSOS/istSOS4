# istSOS4 installation

The easiest way to install istSOS4 is to use Docker. The following steps will guide you through the process.

## Prerequisites

You should have Docker installed on your machine. If you don't have Docker installed, you can download it from the [official website](https://docs.docker.com/get-docker/).
For further information on how to install Docker, refer to the [Docker documentation](https://docs.docker.com/get-docker/).

## Jupiter Notebook

Additionally, to run the hands-on tutorial, you should have Jupiter Notebook installed on your machine. If you don't have Jupiter Notebook installed, you can download it from the [official website](https://jupyter.org/install). 
Alternatively, you can use the Docker image with Jupiter Notebook pre-installed. To run the Docker image with Jupiter Notebook, run the following command:

```sh

docker run -it --rm -p 10000:8888 \
-v "${PWD}":/home/jovyan/work quay.io/jupyter/datascience-notebook:latest \
-c start-notebook.py --IdentityProvider.token=''

```


## Clone the istSOS4repository

```sh
git clone https://github.com/istSOS/istSOS4.git
```

## Start DEV environment

To start the Docker services, run:

```sh
docker compose --env-file .env.dev -f dev_docker-compose.yml up -d
```

To switch off the services:

```sh
docker compose -f dev_docker-compose.yml down
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


```sh 
    git clone 
```
