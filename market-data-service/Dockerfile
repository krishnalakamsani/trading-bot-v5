FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY backend/app/requirements.txt .
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy all market data service code
COPY backend/app/ .
COPY market-data-service/ .

EXPOSE 8001

CMD ["python3", "/app/mds_service_runner.py"]
