# istSOS4 Database

The database follows the SensorThings API (STA) standard data model.

Additionally, other SOS-specific metadata may be considered as an extension.

### Database versioning

You can enable or disable database versioning by setting the **VERSIONING** environment variable in the `.env` file.

For more information about the database versioning, refer to the [Database Versioning Documentation](https://github.com/istSOS/istsos4/blob/traveltime/database/README_VERSIONING.md)

### Datastream aggregation functions

The database image installs the Datastream aggregation functions and procedures
only when versioning is enabled (`VERSIONING=1`) and duplicate entities are
disabled (`DUPLICATES=0`). The aggregation upsert requires the Observation
uniqueness constraint created by the latter setting, while incremental
recomputation uses the system-time history installed by versioning.

The administrator `POST /AggregationJobs` endpoint accepts optional completeness
rules. For example, a daily average from hourly observations can require at least
20 valid numeric readings and at most three consecutive missing hourly slots:

```json
{
  "sourceDatastream": "temperature-hourly",
  "targetDatastream": "temperature-daily",
  "bucketInterval": "1 day",
  "aggregation": "AVG",
  "minObservations": 20,
  "expectedObservationInterval": "1 hour",
  "maxConsecutiveMissing": 3
}
```

`minObservations` and `maxConsecutiveMissing` must be non-negative integers.
`expectedObservationInterval` must be a positive PostgreSQL interval no longer
than `bucketInterval`, and is required when `maxConsecutiveMissing` is set,
including zero. Missing-slot checks include gaps at both bucket boundaries.
Only numeric observations passing the quality filters count toward completeness.
A bucket failing either rule is written with result `-999.9` and quality `0`.
Omitting these settings or setting them to `null` disables the respective checks.

The API maps these fields to the database job configuration's `min_observations`,
`expected_observation_interval`, and `max_consecutive_missing` keys. Job reads
return the configured values, or `null` when unset. `PATCH /AggregationJobs(id)`
continues to accept only `enabled`.
    
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
