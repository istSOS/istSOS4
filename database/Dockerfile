FROM postgis/postgis:16-3.4-alpine
COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
COPY ./istsos_auth.sql /docker-entrypoint-initdb.d/12_init_auth.sql
COPY ./istsos_schema_versioning.sql /docker-entrypoint-initdb.d/13_init_db_istsos_schema_versioning.sql
