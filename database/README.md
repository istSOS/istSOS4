# istSOS4 Database

The database follows the SensorThings API (STA) standard data model.

Additionally, other SOS-specific metadata may be considered as an extension.

### Database versioning

You can enable or disable database versioning by setting the **VERSIONING** environment variable in the `.env` file.

For more information about the database versioning, refer to the [Database Versioning Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README_VERSIONING.md)

### Aggregation configurations

The administrator `/Aggregations` API manages one configuration
per source Datastream. It replaces `/AggregationJobs`; it does not create or
modify derived Datastreams. PATCH supports all editable
settings, and DELETE removes only the configuration. See the
[aggregation schema](README_aggregation.md#table-datastream_configuration)
for the fields.

New database images install the aggregation schema automatically: `database/Dockerfile`
loads `istsos_aggregation.sql` (tables, constraints and watermark are described in
[README_aggregation.md](README_aggregation.md)).
Existing databases are not migrated: rebuild them from the image. All six intervals are
predefined. The configuration table is
available with or without versioning and with duplicate names allowed.

The table `aggregation.datastream_configuration` has a unique, non-null
`datastream_id` foreign key to `sensorthings."Datastream"`, with
`ON DELETE CASCADE`. It is owned by `administrator` and is not included in
SensorThings system-time versioning. The required `min_input_quality` has no
default; nullable processing overrides are left for the future worker to resolve.

The TimescaleDB extension is installed but no hypertables are used: all tables,
including Observation, are plain PostgreSQL tables. Time bucketing uses the core
`date_bin()` where SQL needs it. The legacy SQL aggregation procedures
(`functions.sql`) have been removed. The result
schema creates six shared tables for 10 minutes, one hour,
one day, seven days, one calendar month and one calendar year. See the
[aggregation schema](README_aggregation.md#result-tables) for columns and
constraints. Calculation and refresh are performed by the Python worker;
there are no automatic aggregation jobs or retention policies; results are read through `GET /Datastreams(id)/Aggregates`.
    
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

### Aggregation processing state

`aggregation.processing_state` stores the latest run status per configuration,
including errors, timestamps, consecutive failures and a durable processing checkpoint.
It is a normal table, managed by the Python service and exposed read-only via
`/Aggregations(id)/ProcessingState`. A trigger creates the state row of each
new configuration. Configuration deletion cascades to
its state. See the [aggregation schema](README_aggregation.md#table-processing_state).

`istsos_aggregation.sql` also installs a privileged, read-only safe system-time
watermark function for late-commit handling. The service performs pandas calculations, transactional bucket/cursor
updates and versioned historical change detection; see the
[worker guide](../aggregation/README.md).
