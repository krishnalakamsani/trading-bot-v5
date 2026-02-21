FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY ./backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy backend code (we run the market-data runner from backend)
COPY ./backend /app/backend

EXPOSE 8001

CMD ["python3", "/app/backend/mds_service_runner.py"]
