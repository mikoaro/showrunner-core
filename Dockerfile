# ==========================================
# Stage 1: Builder
# ==========================================
# STRICT REQUIREMENT: Python 3.14.0 Slim
FROM python:3.14.0-slim AS builder

# Prevent Python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies required for building Python packages
# (e.g., gcc for compiling C extensions if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies into a virtual environment
RUN python -m venv /opt/venv
# Enable venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ==========================================
# Stage 2: Runtime
# ==========================================
FROM python:3.14.0-slim AS runtime

WORKDIR /app

# Create a non-root user for security (Best Practice)
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Enable venv in this stage
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY . .

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose the port (Cloud Run defaults to 8080)
ENV PORT=8080
EXPOSE 8080

# Run FastAPI with Uvicorn
# Workers are set to 1 for Cloud Run (concurrency is handled by the platform)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
