from fastapi import FastAPI
from app.v1 import api


app = FastAPI(debug=True)

app.mount("/v1.1", api.v1)

# API SERVIZI
# app.mount("/admin", api.admin)