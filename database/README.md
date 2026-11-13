# istSOS4 Database

The database follows the SensorThings API (STA) standard data model.

Additionally, other SOS-specific metadata may be considered as an extension.

### Database versioning

You can enable or disable database versioning by setting the **VERSIONING** environment variable in the `.env` file.

For more information about the database versioning, refer to the [Database Versioning Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README_VERSIONING.md)

### Aggregation configurations

The administrator `/AggregationConfigurations` API manages one configuration
per source Datastream. It replaces `/AggregationJobs`; it does not create or
modify TimescaleDB jobs or derived Datastreams. PATCH supports all editable
settings, and DELETE removes only the configuration. See the
[API contract](../docs/mkdocs/docs/aggregation-configurations.md) for fields,
examples and response codes.

New database installations load `aggregation_configuration.sql` and
`aggregation_results.sql`, `aggregation_processing_state.sql` and
`aggregation_engine.sql` automatically. For an existing installation, apply
the four files in that order using the database owner or a superuser, with
`ON_ERROR_STOP` enabled. They are idempotent and do not remove
existing jobs, functions, observations or results. The configuration script
removes the initial API's obsolete `intervals` column; all six intervals are
now predefined. The configuration table is
available with or without versioning and with duplicate names allowed.

The table `aggregation.datastream_configuration` has a unique, non-null
`datastream_id` foreign key to `sensorthings."Datastream"`, with
`ON DELETE CASCADE`. It is owned by `administrator` and is not included in
SensorThings system-time versioning. The required `min_input_quality` has no
default; nullable processing overrides are left for the future worker to resolve.

Legacy SQL aggregation procedures remain in `functions.sql` for installations
that still use them. Removing the old API routes does not unregister database
jobs; production cleanup is managed separately. The result schema creates six shared hypertables for 10 minutes, one hour,
one day, seven days, one calendar month and one calendar year. See the
[storage contract](../docs/mkdocs/docs/aggregation-results.md) for columns and
constraints. Calculation and refresh are performed by the Python worker;
there are no automatic aggregation jobs, CAGGs, retention policies or result API.
    
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
`/AggregationConfigurations(id)/ProcessingState`. Apply
`aggregation_processing_state.sql` after configuration storage on existing
databases. A trigger initializes new rows; existing configurations are
backfilled without resetting current state. Configuration deletion cascades to
its state. See the [API contract](../docs/mkdocs/docs/aggregation-configurations.md#processing-state).

`aggregation_engine.sql` installs a privileged, read-only safe system-time
watermark function for late-commit handling. Apply it after processing-state
storage. The service performs pandas calculations, transactional bucket/cursor
updates and versioned historical change detection; see the
[worker guide](../aggregation/README.md).
