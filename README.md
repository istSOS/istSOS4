# Quick start

## Start docker service

```
docker compose up -d
```

## Use Sensor Things APIs

http://127.0.0.1:8018/istsos-miu/v1.1

## Database

The database should reflect the STA standard data model additionally other SOS specific metadata could be considered as and extension.

### Database versioning

Activate/disactivate versioning by set **VERSIONING** environment variable inside ```.env``` file.
    
### Database fake data

When you build the docker the script will automatically clear the database and add the static and dynamic values. 

To disabling addtion of the synthetic data to database, inside the ```/dummy_data/config.yaml``` file change the variable  **dummy_data** to **False**.

To disabling clean of the synthetic data, inside the /dummy_data/config.yaml file change the variable  **clear_data** to **False**.

### Connect to database in DBEAVER

Create New database connection with PostgreSQL as driver:

- Host: 127.0.0.1
- Port: 45432
- Database: istsos (POSTGRES_DB variable inside ```.env``` file
- Username: admin (POSTGRES_USER variable inside ```.env``` file
- Password: admin (POSTGRES_PASSWORD variable inside ```.env``` file
