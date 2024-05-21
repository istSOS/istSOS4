# Quick start

## Start docker service

To start the Docker service, run:

```sh
docker compose up -d
```

## Use Sensor Things APIs

Access the SensorThings API at:

http://127.0.0.1:8018/istsos-miu/v1.1

## Database

The database follows the SensorThings API (STA) standard data model. Additionally, other SOS-specific metadata may be considered as an extension.

### Database versioning

You can enable or disable database versioning by setting the VERSIONING environment variable in the `.env` file.
    
### Database fake data

When you build the Docker image, the script will automatically clear the database and add static and dynamic values. 

To disable the addition of these data, set **dummy_data** to *False* in the `/dummy_data/config.yaml` file.

To prevent clearing the database, set **clear_data** to *False* in the `/dummy_data/config.yaml` file.

### Connect to database in DBeaver

To create a new database connection in DBeaver using PostgreSQL as the driver, use the following settings:

- **Host**: 127.0.0.1
- **Port**: 45432
- **Database**: istsos (controlled by the **POSTGRES_DB** variable in the `.env` file)
- **Username**: admin (controlled by the **POSTGRES_USER** variable in the `.env` file)
- **Password**: admin (controlled by the **POSTGRES_PASSWORD** variable in the `.env` file)
