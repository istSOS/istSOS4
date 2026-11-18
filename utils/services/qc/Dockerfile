# Build from utils/, to share qc_historical:
#   docker build -t ghcr.io/istsos/istsos4/utils/qc:0.1 -f services/qc/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# saqc imports tkinter
RUN apt-get update \
    && apt-get install -y --no-install-recommends libtk8.6 \
    && rm -rf /var/lib/apt/lists/*

COPY jobs/qc_historical/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/qc_historical/qc_historical.py services/qc/qc.py ./

ENTRYPOINT ["python", "qc.py"]
