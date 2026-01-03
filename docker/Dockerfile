# syntax=docker/dockerfile:1

# MySQL Database Dumper Dockerfile
# Uses Docker BuildKit features for optimized builds

# ============================================================================
# Stage 1: Base image with system dependencies
# ============================================================================
FROM python:3.12-slim AS base

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies (mysql-client for mysqldump)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        default-mysql-client \
        && rm -rf /var/lib/apt/lists/*

# ============================================================================
# Stage 2: Builder - Install Python dependencies
# ============================================================================
FROM base AS builder

WORKDIR /app

# Copy only requirements first for better cache utilization
COPY requirements.txt .

# Install Python dependencies with cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --user -r requirements.txt

# ============================================================================
# Stage 2a: Dev - For Development & Testing
# ============================================================================
FROM builder AS dev

WORKDIR /app

# Copy only requirements first for better cache utilization
COPY requirements-dev.txt .

# Install Python dependencies with cache mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --user -r requirements-dev.txt

# ============================================================================
# Stage 3: Final runtime image
# ============================================================================
FROM base AS runtime

# Create non-root user for security
RUN groupadd --gid 1000 dumper && \
    useradd --uid 1000 --gid dumper --shell /bin/bash --create-home dumper

WORKDIR /app

# Copy installed Python packages from builder
COPY --from=builder /root/.local /home/dumper/.local

# Update PATH to include user-installed packages
ENV PATH=/home/dumper/.local/bin:$PATH

# Copy application files
COPY --chown=dumper:dumper src/ src/
COPY --chown=dumper:dumper config.example.yaml .

# Create dumps directory with correct ownership
RUN mkdir -p /app/dumps && chown dumper:dumper /app/dumps

# Set default volume for dumps output
VOLUME ["/app/dumps"]

# Switch to non-root user
USER dumper

# Health check - verify Python and mysql client are available
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python --version && mysql --version || exit 1

# Default entrypoint - run as package module
ENTRYPOINT ["python", "-m", "src.main"]

# Default command (can be overridden)
CMD ["--help"]
