# Start from a robust Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies (PostgreSQL client needed for psycopg2, gcc for compiling)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy core Python scripts to the app root
COPY task2_build.py /app/
COPY task2_validate.py /app/

# Set non-root user
RUN useradd -m appuser
USER appuser

# Entry point is bash 
ENTRYPOINT ["/bin/bash"]