from app.v1.endpoints import delete, insert, read, update_patch

from fastapi import FastAPI

v1 = FastAPI()

# Register the endpoints
v1.include_router(read.v1)
v1.include_router(insert.v1)
v1.include_router(delete.v1)
v1.include_router(update_patch.v1)
