FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY bot/     ./bot/
COPY dashboard/ ./dashboard/

# Create log directory
RUN mkdir -p /app/logs /app/data

# Set PYTHONPATH so imports resolve
ENV PYTHONPATH=/app/bot:/app

EXPOSE 8080 8081

WORKDIR /app/bot
CMD ["python", "main.py"]
