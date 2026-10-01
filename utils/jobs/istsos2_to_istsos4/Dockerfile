FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY istsos2_client.py istsos2_to_istsos4.py ./

CMD ["python", "istsos2_to_istsos4.py"]
