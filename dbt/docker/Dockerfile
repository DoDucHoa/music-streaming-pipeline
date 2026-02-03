# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /usr/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-bigquery with compatible versions
RUN pip install --no-cache-dir \
    protobuf==4.25.1 \
    google-cloud-bigquery==3.11.4 \
    dbt-bigquery==1.7.0 \
    dbt-core==1.7.0

# Copy dbt project files
COPY dbt_project.yml .
COPY profiles.yml .
COPY packages.yml .

# Copy models, macros, tests
COPY models/ models/
COPY macros/ macros/
COPY tests/ tests/

# Install dbt packages
RUN dbt deps

# Set default command
ENTRYPOINT ["dbt"]
CMD ["--help"]
