# ============================================================================
# Educational Insights Platform - Dockerfile
# ============================================================================
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY ui/ ./ui/
COPY sql/ ./sql/
COPY data/ ./data/

# Copy entrypoint
COPY .env.example .env.example

# Create non-root user
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Expose Streamlit default port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Default command: run Streamlit UI
CMD ["streamlit", "run", "ui/streamlit_app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
