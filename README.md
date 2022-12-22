# CREATE FASTAPI

```
docker-compose up -d
```

## See the automatic interactive API documentation

http://127.0.0.1:8018/docs

## Install httpx for test

```
pip install httpx
```

## Change IP address for test

Inside app/test_main.py file insert yours IP address at line 13:

pgpool = await asyncpg.create_pool(dsn='postgresql://admin:admin@<IP_ADDRESS>:55432/istsos3_test')

## Run test

```
cd /app
pytest
```
