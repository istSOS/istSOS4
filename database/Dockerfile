FROM timescale/timescaledb-ha:pg18-ts2.25
COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
COPY ./istsos_auth.sql /docker-entrypoint-initdb.d/12_init_auth.sql
COPY ./istsos_schema_versioning.sql /docker-entrypoint-initdb.d/13_init_db_istsos_schema_versioning.sql
COPY ./functions.sql /docker-entrypoint-initdb.d/14_init_aggregation_functions.sql
COPY ./aggregation_configuration.sql /docker-entrypoint-initdb.d/15_init_aggregation_configuration.sql
COPY ./aggregation_results.sql /docker-entrypoint-initdb.d/16_init_aggregation_results.sql

COPY ./aggregation_processing_state.sql /docker-entrypoint-initdb.d/17_init_aggregation_processing_state.sql
COPY ./aggregation_engine.sql /docker-entrypoint-initdb.d/18_init_aggregation_engine.sql
