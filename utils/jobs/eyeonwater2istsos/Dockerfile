FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY eyeonwater2istsos.py fetch_eyeonwater2istsos.py ./

# Two entry points: run.sh -> eyeonwater2istsos.py, fetch.sh -> fetch_eyeonwater2istsos.py
ENTRYPOINT ["python"]
