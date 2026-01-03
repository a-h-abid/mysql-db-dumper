# Docker Setup for MySQL Database Dumper

This document describes how to use Docker to run the MySQL Database Dumper.

## Prerequisites

- Docker 20.10+ (with BuildKit support)
- Docker Compose v2.0+

## Quick Start

> **Note:** The Dockerfile and compose file are located in the `docker/` directory. From the project root you can run:
> - `docker compose -f docker/docker-compose.yml <command>` or
> - `cd docker && docker compose <command>`

### 1. Setup Configuration

```bash
# Copy example files
cp config.example.yaml config.yaml
cp docker/.env.example docker/.env

# Edit configuration with your database details
nano config.yaml
nano docker/.env
```

### 2. Build the Image

```bash
# Build with BuildKit (recommended)
docker compose build

# Or build directly with Docker
DOCKER_BUILDKIT=1 docker build -t mysql-db-dumper .
```

### 3. Run the Dumper

```bash
# Run with docker compose
docker compose run --rm dumper

# Run with specific arguments
docker compose run --rm dumper --config /app/config.yaml --instance primary

# Run interactively
docker compose run --rm -it dumper --help
```

## Usage Examples

### Dump All Configured Databases

```bash
docker compose run --rm dumper --config /app/config.yaml
```

### Dump Specific Database

```bash
docker compose run --rm dumper --config /app/config.yaml --database mydb
```

### Dump with Custom Output Directory

```bash
docker compose run --rm -v /custom/path:/app/dumps dumper --config /app/config.yaml
```

### Run with Environment Variables

```bash
docker compose run --rm \
  -e MYSQL_PRIMARY_PASSWORD=secret \
  dumper --config /app/config.yaml
```

## Docker Compose Commands

```bash
# Build image
docker compose build

# Build with no cache
docker compose build --no-cache

# View logs
docker compose logs dumper

# Stop all services
docker compose down

# Stop and remove volumes
docker compose down -v

# Run one-off command
docker compose run --rm dumper python -c "print('Hello')"

# Run Unit Tests
docker compose run --rm dumper python -m pytest tests/ -v
```

## BuildKit Features Used

This Dockerfile uses several BuildKit features:

1. **Cache Mounts**: APT and pip caches are mounted for faster builds
   ```dockerfile
   RUN --mount=type=cache,target=/var/cache/apt ...
   RUN --mount=type=cache,target=/root/.cache/pip ...
   ```

2. **Multi-stage Builds**: Separate builder and runtime stages for smaller images

3. **Registry Cache**: Build cache is stored in a container registry for sharing across machines and CI/CD pipelines
   ```yaml
   cache_from:
     - type=registry,ref=docker.io/mysql-db-dumper:buildcache
   cache_to:
     - type=registry,ref=docker.io/mysql-db-dumper:buildcache,mode=max
   ```

### Builder Requirement

The registry cache backend requires a builder with a driver other than the default `docker` driver. Create a buildx builder before building:

```bash
# Create and use a new builder with docker-container driver
docker buildx create --use --name mybuilder

# Verify the builder is active
docker buildx ls
```

## Security Features

- **Non-root user**: Application runs as `dumper` user (UID 1000)
- **Read-only filesystem**: Root filesystem is read-only
- **No new privileges**: Prevents privilege escalation
- **Resource limits**: CPU and memory limits configured
- **Tmpfs mounts**: Temporary files stored in memory

## Volumes

| Volume | Purpose |
|--------|---------|
| `./config.yaml:/app/config.yaml:ro` | Configuration file (read-only) |
| `./dumps:/app/dumps` | Output directory for database dumps |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_PRIMARY_HOST` | localhost | Primary MySQL host |
| `MYSQL_PRIMARY_PORT` | 3306 | Primary MySQL port |
| `MYSQL_PRIMARY_USER` | root | Primary MySQL user |
| `MYSQL_PRIMARY_PASSWORD` | - | Primary MySQL password |
| `TZ` | UTC | Container timezone |

## Troubleshooting

### Permission Denied on Dumps Directory

```bash
# Fix ownership
sudo chown -R 1000:1000 ./dumps
```

### Cannot Connect to MySQL

1. Ensure MySQL host is accessible from Docker container
2. For local MySQL, use `host.docker.internal` (Docker Desktop) or the host's IP
3. Check firewall settings

### Build Cache Issues

```bash
# Clear build cache
docker builder prune

# Rebuild without cache
docker compose build --no-cache
```

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Build and push
  uses: docker/build-push-action@v5
  with:
    context: .
    push: true
    tags: myregistry/mysql-db-dumper:latest
    cache-from: type=gha
    cache-to: type=gha,mode=max
```

### Build with Cache Export

```bash
docker buildx build \
  --cache-to type=local,dest=/tmp/cache \
  --cache-from type=local,src=/tmp/cache \
  -t mysql-db-dumper .
```

## Container Entrypoint

The Docker container uses `python -m src.main` as its entrypoint, running the application as a proper Python package module. This ensures correct handling of relative imports across the modular codebase.
