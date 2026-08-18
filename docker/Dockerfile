# Stage 1: Build stage
FROM python:3.11-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /build/wheels -r requirements.txt

# Stage 2: Runtime stage
FROM python:3.11-slim as runtime

# Create non-root user for security
RUN groupadd -r streamcore && useradd -r -g streamcore streamcore

WORKDIR /app

# Copy wheels from builder and install
COPY --from=builder /build/wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Copy application code
COPY services/ ./services/
COPY shared/ ./shared/
COPY pyproject.toml .

# Create log directory and set permissions
RUN mkdir -p /data/logs && chown -R streamcore:streamcore /data

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMCORE_LOG_DIR=/data/logs \
    STREAMCORE_SEGMENT_SIZE=1048576 \
    STREAMCORE_PORT=8000

# Switch to non-root user
USER streamcore

# Expose the broker port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()" || exit 1

# Run the broker
CMD ["python", "-m", "services.broker.server"]
