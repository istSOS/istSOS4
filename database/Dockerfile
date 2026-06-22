FROM timescale/timescaledb-ha:pg18-ts2.25
COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
COPY ./istsos_auth.sql /docker-entrypoint-initdb.d/12_init_auth.sql
COPY ./istsos_schema_versioning.sql /docker-entrypoint-initdb.d/13_init_db_istsos_schema_versioning.sql
COPY ./istsos_staplus.sql /docker-entrypoint-initdb.d/14_init_db_istsos_staplus.sql
