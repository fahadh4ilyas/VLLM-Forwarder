# Using the slim variant saves a lot of image space
FROM python:3.11-slim 

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Jakarta

RUN apt-get update && apt-get install -y \
    tzdata \
    && rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

WORKDIR /api

# 1. DEPENDENCY CACHING: Copy requirements and install FIRST
COPY ./requirements.txt /api/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2. COPY APPLICATION CODE
COPY ./source/app /api/app
COPY ./source/uvicorn.main.py /api/uvicorn.main.py

ENV PYTHONPATH=/api

# 3. DATA PERSISTENCE: Ensure the db directory exists and is marked as a volume
RUN mkdir -p /api/db /api/logs
VOLUME ["/api/db", "/api/logs"]

# 4. PORT EXPOSURE: Explicitly declare the port (defaults to 5000 from config.py)
EXPOSE 5000

CMD ["python", "uvicorn.main.py"]