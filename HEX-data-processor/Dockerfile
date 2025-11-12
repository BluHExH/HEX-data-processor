# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r hexuser && useradd -r -g hexuser hexuser

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY config_example.json .
COPY config_schema.json .

# Create necessary directories
RUN mkdir -p data/input data/output logs && \
    chown -R hexuser:hexuser /app

# Switch to non-root user
USER hexuser

# Expose metrics port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["python", "-m", "src.main", "config_example.json", "quotes_toscrape"]

# Alternative commands (commented out, use as needed)
# CMD ["python", "-m", "src.cli", "--config", "config_example.json", "run", "quotes_toscrape", "--once"]
# CMD ["python", "-m", "src.cli", "--config", "config_example.json", "serve", "--host", "0.0.0.0", "--port", "8000"]