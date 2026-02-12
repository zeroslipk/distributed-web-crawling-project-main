FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for MPI, GSSAPI (if needed), and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    openmpi-bin \
    libopenmpi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies and Gunicorn for production
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy the entire project structure
COPY src/ src/
COPY templates/ templates/
COPY config/ config/
COPY data/ data/
# Create a folder for logs if it doesn't exist
RUN mkdir -p logs

# Set working directory to where the app can find src/
WORKDIR /app

# Expose the port (Render uses PORT env var, but we'll default to 8080)
EXPOSE 8080

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Use Gunicorn for production
# We need to tell gunicorn where the app is. In app.py it is just 'app'
# We'll run it from the root directory so it can find 'templates/app.py'
CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 templates.app:app