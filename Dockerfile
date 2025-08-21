FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends zip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV LOG=1 \
    LOG_LEVEL=INFO \
    THROTTLE_KBPS=0 \
    PHOTOS_DIR=photos \
    PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["python", "server.py"]
