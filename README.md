# Music Streaming Data Pipeline

> **End-to-end real-time data engineering pipeline** processing streaming events from ingestion to analytics-ready insights

A production-grade data platform that simulates a music streaming service, processes millions of user events in real-time, and transforms raw data into actionable business intelligence using modern data engineering tools.

---

## 🎯 Project Overview

This project demonstrates a complete data engineering solution for a music streaming platform, handling the full lifecycle of event data from generation through real-time processing to analytical insights.

**Business Problem**: Music streaming platforms generate millions of user events daily (song plays, skips, sessions, etc.). This pipeline processes these events in real-time, stores them efficiently in a cloud data warehouse, and transforms them into analytics-ready tables for business intelligence.

**Solution**: An automated, scalable pipeline that:
- Generates realistic user behavior events (1000+ concurrent users)
- Streams events through Apache Kafka for reliable message delivery
- Processes data in real-time using Apache Spark Structured Streaming
- Stores processed data in Google BigQuery with optimized partitioning
- Transforms raw events into dimensional models using dbt
- Orchestrates batch workflows with Apache Airflow

---

## 📊 Key Achievements

- **863,937+ events processed** with zero data loss
- **100% test coverage** (145/145 dbt tests passing)
- **11 analytics models** spanning 4 transformation layers
- **3 automated DAGs** with email notifications
- **Cross-platform compatibility** (Windows, macOS, Linux)
- **Production-ready code** with comprehensive documentation

---

## 🛠️ Technologies Used

| Layer | Technologies |
|-------|-------------|
| **Event Generation** | Python, Faker library, Kafka Producer API |
| **Message Streaming** | Apache Kafka, Zookeeper |
| **Real-time Processing** | Apache Spark 3.5 (Structured Streaming, PySpark) |
| **Data Warehouse** | Google BigQuery (partitioned & clustered tables) |
| **Analytics Transformation** | dbt (SQL, Jinja templating) |
| **Orchestration** | Apache Airflow 2.8 (LocalExecutor, PostgreSQL) |
| **Infrastructure** | Docker, Docker Compose |
| **Cloud Platform** | Google Cloud Platform (BigQuery, IAM) |

---

## 🏗️ Architecture

```
Python Generator → Kafka → Spark Streaming → BigQuery → dbt → Airflow → Analytics/BI
```

The pipeline follows a modern data platform architecture:

1. **Ingestion Layer**: Python event generator publishes realistic user events to Kafka topics
2. **Streaming Layer**: Spark Structured Streaming consumes from Kafka, applies transformations
3. **Storage Layer**: BigQuery stores partitioned data with automatic schema enforcement
4. **Analytics Layer**: dbt transforms raw events into facts, dimensions, and aggregations
5. **Orchestration Layer**: Airflow schedules daily/hourly jobs and data quality checks

---

## ✨ Key Features

### Real-time Event Processing
- Processes 3,000-3,500 events per batch (30-second intervals)
- Handles multiple event types: song plays, page views, authentication, status changes
- Data quality filters and validation rules
- Fault-tolerant with checkpoint recovery

### Analytics Transformation (dbt)
- **4-layer architecture**: Staging → Intermediate → Facts/Dimensions → Aggregations
- **Incremental materialization** with 3-day lookback for late-arriving data
- **Session analysis**: User engagement metrics, session duration, skip rates
- **Song analytics**: Play counts, artist popularity, listening patterns
- **User segmentation**: Power users, casual listeners, conversion tracking

### Automated Orchestration (Airflow)
- **Daily full refresh** (2:00 AM): Complete model rebuild
- **Hourly incremental** (every hour): Fresh metrics for dashboards
- **Data quality suite** (every 6 hours): Automated testing
- **Email alerts**: Failure notifications via SMTP

### Production-Ready Infrastructure
- Containerized deployment with Docker Compose
- Partitioned BigQuery tables for query performance
- Clustered columns for optimal filtering
- Environment-based configuration (.env files)
- Comprehensive logging and monitoring

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Google Cloud Platform account (free tier supported)
- 8GB RAM, 20GB disk space

### Setup (5 minutes)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd music-streaming-pipeline
   ```

2. **Configure environment**
   ```bash
   # Copy template and add your GCP project ID
   cp .env.template .env
   # Edit .env and set GCP_PROJECT_ID
   ```

3. **Add GCP credentials**
   - Place your service account key at `spark-streaming/credentials/gcp-service-account-key.json`
   - See [GCP Setup Guide](docs/personal-local-docs/GCP_SETUP_GUIDE.md) for details

4. **Start the pipeline**
   ```bash
   # Start all services
   docker compose up -d
   
   # Verify data flow
   docker logs spark-streaming --tail=50 -f
   ```

5. **Access interfaces**
   - Spark UI: http://localhost:4040 (streaming jobs)
   - Airflow UI: http://localhost:8080 (username: admin / password: admin)
   - BigQuery Console: https://console.cloud.google.com/bigquery

### Run dbt Transformations

```bash
# Full refresh (first time or with historical data)
docker compose run --rm dbt run --full-refresh

# Incremental updates
docker compose run --rm dbt run

# Run tests
docker compose run --rm dbt test

# Generate documentation
docker compose run --rm dbt docs generate
docker compose run --rm dbt docs serve --port 8080
```

---

## 📁 Project Structure

```
music-streaming-pipeline/
├── python-event-generator/   # Event simulation (487 lines)
│   ├── event_generator.py    # State machine with probabilistic transitions
│   └── data/                 # 385,252 songs with durations
├── spark-streaming/          # Real-time processing (1,500+ lines)
│   ├── src/processors/       # Kafka consumer, transformers, BigQuery sink
│   ├── config/config.yaml    # Comprehensive Spark configuration
│   └── docker/              # Dockerfile + entrypoint (225 lines)
├── dbt/                     # Analytics layer (3,200+ lines)
│   ├── models/              # 11 SQL models across 4 layers
│   │   ├── staging/         # Data cleaning (2 models)
│   │   ├── intermediate/    # Business logic (2 models)
│   │   ├── marts/core/      # Facts (2 models)
│   │   ├── marts/dimensions/# Dimensions (2 models)
│   │   └── marts/analytics/ # Aggregations (3 models)
│   └── tests/              # 145 automated tests
├── airflow/                # Orchestration (3 DAGs, 27 tasks)
│   └── dags/              # Daily, hourly, and test workflows
└── docker-compose.yml     # 9-service orchestration
```

---

## 📈 Data Flow

1. **Event Generation**: Python generator simulates 1,000 concurrent users with realistic behavior patterns
2. **Message Queuing**: Events published to Kafka topic `listen_events`
3. **Stream Processing**: Spark reads from Kafka every 30 seconds, applies transformations
4. **Data Warehouse**: Processed events written to BigQuery `raw_events` table
5. **Analytics Models**: dbt transforms raw data into:
   - `fct_song_plays`: Song-level play tracking
   - `fct_sessions`: Session metrics and engagement
   - `dim_users`: User profiles and segments
   - `dim_songs`: Song metadata and popularity
   - `agg_daily_metrics`: Platform KPIs (skip rate, conversion rate)
   - `agg_hourly_activity`: Peak usage patterns
   - `agg_user_summary`: User lifetime value
6. **Orchestration**: Airflow schedules daily/hourly refreshes and monitors data quality
