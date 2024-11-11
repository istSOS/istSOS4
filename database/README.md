# istSOS4 Database

The database follows the SensorThings API (STA) standard data model.

Additionally, other SOS-specific metadata may be considered as an extension.

### Database versioning

You can enable or disable database versioning by setting the **VERSIONING** environment variable in the `.env` file.

For more information about the database versioning, refer to the [Database Versioning Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README_VERSIONING.md)
    
### Database dummy data

You can enable or disable the addition of dummy data by setting **DUMMY_DATA** environment variable in the `.env` file.

You can enable or disable the cleaning by setting **CLEAR_DATA** environment variable in the `.env` file.

For more information about the database dummy data, refer to the [Database Dummy Data Documentation](https://github.com/istSOS/istsos4/blob/traveltime/dummy_data/README.md)

### Connect to database in DBeaver

To create a new database connection in DBeaver using PostgreSQL as the driver, use the following settings:

- **Host**: 127.0.0.1
- **Port**: 45432
- **Database**: istsos (controlled by the **POSTGRES_DB** variable in the `.env` file)
- **Username**: admin (controlled by the **POSTGRES_USER** variable in the `.env` file)
- **Password**: admin (controlled by the **POSTGRES_PASSWORD** variable in the `.env` file)
