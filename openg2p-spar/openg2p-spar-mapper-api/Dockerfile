FROM python:3.11-slim

ARG container_user=openg2p
ARG container_user_group=openg2p
ARG container_user_uid=1001
ARG container_user_gid=1001

# Create non-root user
RUN groupadd -g ${container_user_gid} ${container_user_group} && \
    useradd -mN -u ${container_user_uid} -G ${container_user_group} -s /bin/bash ${container_user}

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    gunicorn \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

# Copy all local packages and application code
ADD --chown=${container_user}:${container_user_group} openg2p-spar-models /app/openg2p-spar-models
ADD --chown=${container_user}:${container_user_group} openg2p-spar-mapper-core /app/openg2p-spar-mapper-core
ADD --chown=${container_user}:${container_user_group} openg2p-spar-mapper-api /app/openg2p-spar-mapper-api

# Install Python dependencies
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir \
      git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.4#subdirectory=openg2p-fastapi-auth-models \
      git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.4#subdirectory=openg2p-fastapi-auth \
      git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.4#subdirectory=openg2p-fastapi-common \
      git+https://github.com/openg2p/openg2p-fastapi-common@v1.1.4#subdirectory=openg2p-fastapi-partner-auth && \
    python -m pip install --no-cache-dir -e /app/openg2p-spar-models && \
    python -m pip install --no-cache-dir -e /app/openg2p-spar-mapper-core && \
    python -m pip install --no-cache-dir -e /app/openg2p-spar-mapper-api

# Switch to non-root user
USER ${container_user}

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV SPAR_MAPPER_API_HOST=0.0.0.0
ENV SPAR_MAPPER_API_PORT=8000
ENV SPAR_MAPPER_API_NO_OF_WORKERS=4

# Set working directory to the mapper API
WORKDIR /app/openg2p-spar-mapper-api

# Run the application
CMD ["sh", "-c", "gunicorn main:app --workers ${SPAR_MAPPER_API_NO_OF_WORKERS} --worker-class uvicorn.workers.UvicornWorker --bind ${SPAR_MAPPER_API_HOST}:${SPAR_MAPPER_API_PORT} --access-logfile - --error-logfile -"]
