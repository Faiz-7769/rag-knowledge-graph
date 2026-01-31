FROM python:3.10-slim

WORKDIR /code

# Environment variables for HF Spaces
ENV CHAINLIT_HOST=0.0.0.0
ENV CHAINLIT_PORT=7860
ENV CHAINLIT_TELEMETRY=false
ENV PYTHONUNBUFFERED=1

# Install system dependencies (optional but safe)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Fix Chainlit permissions (HF Spaces requirement)
RUN mkdir -p /.chainlit && chmod -R 777 /.chainlit

# Expose HF default port
EXPOSE 7860

# Start Chainlit
CMD ["chainlit", "run", "app.py", "--watch"]
