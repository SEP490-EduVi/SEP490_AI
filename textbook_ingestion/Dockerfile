FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create temp directory for downloads
RUN mkdir -p temp_downloads

# Run the RabbitMQ consumer
CMD ["python", "main.py"]
