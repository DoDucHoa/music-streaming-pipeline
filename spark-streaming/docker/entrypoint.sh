#!/bin/bash
set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Banner
echo "=================================================="
echo "   Spark Streaming Application - Music Pipeline"
echo "=================================================="
echo ""

# Validate required environment variables
log_info "Validating environment variables..."

if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "GCP_PROJECT_ID environment variable is not set"
    exit 1
fi
log_success "GCP_PROJECT_ID: $GCP_PROJECT_ID"

if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    log_warning "KAFKA_BOOTSTRAP_SERVERS not set, using default: kafka:29092"
    export KAFKA_BOOTSTRAP_SERVERS="kafka:29092"
fi
log_success "KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"

# Check GCP credentials
log_info "Checking GCP credentials..."
CREDENTIALS_PATH="/opt/spark-app/credentials/gcp-service-account-key.json"

if [ ! -f "$CREDENTIALS_PATH" ]; then
    log_error "GCP credentials file not found at: $CREDENTIALS_PATH"
    log_error "Please mount the credentials file to /opt/spark-app/credentials/"
    exit 1
fi
log_success "GCP credentials file found"

# Set Google Application Credentials
export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_PATH"

# Wait for Kafka to be ready
log_info "Waiting for Kafka to be ready..."
KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f2)

RETRY_COUNT=0
MAX_RETRIES=30
RETRY_DELAY=2

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$KAFKA_HOST/$KAFKA_PORT" 2>/dev/null; then
        log_success "Kafka is ready at $KAFKA_BOOTSTRAP_SERVERS"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
            log_error "Kafka is not available after $MAX_RETRIES attempts"
            log_error "Please ensure Kafka is running and accessible"
            exit 1
        fi
        log_warning "Kafka not ready yet (attempt $RETRY_COUNT/$MAX_RETRIES), retrying in ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    fi
done

# Pre-flight checks
log_info "Running pre-flight checks..."

# Check if config file exists
CONFIG_FILE="/opt/spark-app/config/config.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Configuration file not found at: $CONFIG_FILE"
    exit 1
fi
log_success "Configuration file found"

# Check if main application exists
MAIN_APP="/opt/spark-app/src/main.py"
if [ ! -f "$MAIN_APP" ]; then
    log_error "Main application not found at: $MAIN_APP"
    exit 1
fi
log_success "Main application found"

# Check if pyfiles.zip exists
PYFILES="/opt/spark-app/pyfiles.zip"
if [ ! -f "$PYFILES" ]; then
    log_error "Python files zip not found at: $PYFILES"
    exit 1
fi
log_success "Python files archive found"

# Set default values for optional environment variables
export APP_NAME="${APP_NAME:-music-streaming-pipeline}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export DRY_RUN="${DRY_RUN:-false}"

log_info "Application configuration:"
log_info "  - App Name: $APP_NAME"
log_info "  - Log Level: $LOG_LEVEL"
log_info "  - Dry Run: $DRY_RUN"
log_info "  - GCP Project: $GCP_PROJECT_ID"

# Prepare Spark submit command
log_info "Preparing Spark submit command..."

SPARK_SUBMIT_CMD="spark-submit"

# Add JARs
JARS_DIR="/opt/spark/jars"
BIGQUERY_JAR="$JARS_DIR/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
KAFKA_JAR="$JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar"
KAFKA_CLIENT_JAR="$JARS_DIR/kafka-clients-3.4.1.jar"
COMMONS_POOL_JAR="$JARS_DIR/commons-pool2-2.11.1.jar"
SPARK_TOKEN_JAR="$JARS_DIR/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"

JARS="$BIGQUERY_JAR,$KAFKA_JAR,$KAFKA_CLIENT_JAR,$COMMONS_POOL_JAR,$SPARK_TOKEN_JAR"

# Spark configuration
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD \
    --name \"$APP_NAME\" \
    --master local[*] \
    --jars $JARS \
    --py-files $PYFILES \
    --conf spark.sql.streaming.schemaInference=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    --conf spark.driver.maxResultSize=1g \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider \
    $MAIN_APP"

# Handle different commands
COMMAND="${1:-start}"

case "$COMMAND" in
    start)
        log_info "Starting Spark Streaming application..."
        log_info "Command: $SPARK_SUBMIT_CMD"
        echo ""
        echo "=================================================="
        echo "   Application Starting - Logs Below"
        echo "=================================================="
        echo ""
        
        # Execute Spark submit
        exec $SPARK_SUBMIT_CMD
        ;;
    
    bash)
        log_info "Starting interactive bash shell..."
        exec /bin/bash
        ;;
    
    test)
        log_info "Running in test mode..."
        log_info "Executing: python3 -m pytest /opt/spark-app/tests/"
        cd /opt/spark-app
        exec python3 -m pytest tests/ -v
        ;;
    
    *)
        log_error "Unknown command: $COMMAND"
        log_info "Available commands: start, bash, test"
        exit 1
        ;;
esac
