FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY istsos4_to_istsos4/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY client.py /app/client.py
COPY istsos4_to_istsos4/istsos2istsos.py /app/istsos4_to_istsos4/istsos2istsos.py

WORKDIR /app/istsos4_to_istsos4

CMD ["python", "istsos2istsos.py"]
