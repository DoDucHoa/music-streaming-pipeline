# Dockerfile for Spark Streaming Application
# Base image: Apache Spark official image with Python support
FROM apache/spark:3.5.0

# Switch to root user for installations
USER root

# Set working directory
WORKDIR /opt/spark-app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    less \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
# Using pip to install all required packages
RUN pip install --no-cache-dir -r requirements.txt

# Download and install Google Cloud BigQuery Spark connector
# This is the official connector JAR required for Spark-BigQuery integration
RUN wget -P /opt/spark/jars/ \
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.36.1/spark-3.5-bigquery-0.36.1.jar

# Download Kafka Spark connector (spark-sql-kafka)
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Download Kafka clients library
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

# Download additional required dependencies
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Copy application source code
COPY src/ ./src/
COPY config/ ./config/

# Create a zip file of the source code for Spark workers
# The zip should contain the modules at the root level so workers can import them
RUN cd /opt/spark-app/src && python3 -m zipfile -c /opt/spark-app/pyfiles.zip \
    processors/ utils/ __pycache__/ *.py

# Create directories for checkpoints and logs
RUN mkdir -p /opt/spark-app/checkpoints /opt/spark-app/logs

# Set permissions (Apache Spark image uses spark user with UID 185)
RUN chown -R 185:185 /opt/spark-app

# Copy entrypoint script
COPY docker/entrypoint.sh /opt/spark-app/entrypoint.sh
RUN chmod +x /opt/spark-app/entrypoint.sh

# Switch back to non-root user for security
USER 185

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

# Expose ports
# 4040: Spark UI
# 18080: Spark History Server (optional)
EXPOSE 4040 18080

# Set entrypoint
ENTRYPOINT ["/opt/spark-app/entrypoint.sh"]

# Default command (can be overridden)
CMD ["start"]
