# Apache Airflow Dockerfile
# Music Streaming Pipeline - Orchestration Layer

FROM apache/airflow:2.8.1-python3.11

# Switch to root for system packages
USER root

# Install system dependencies including docker CLI and docker-compose
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
    && curl -fsSL https://get.docker.com -o get-docker.sh \
    && sh get-docker.sh \
    && curl -L "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose \
    && ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* get-docker.sh

# Switch back to airflow user
USER airflow

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow

# Environment variables (can be overridden)
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD [ "airflow", "jobs", "check", "--job-type", "SchedulerJob", "--hostname", "$(hostname)" ]
