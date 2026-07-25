FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY istsos2_to_istsos4/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY client.py /app/client.py
COPY istsos2_to_istsos4/istsos2istsos.py /app/istsos2_to_istsos4/istsos2istsos.py
COPY istsos2_to_istsos4/config.yml /app/istsos2_to_istsos4/config.yml

WORKDIR /app/istsos2_to_istsos4

CMD ["python", "istsos2istsos.py"]
