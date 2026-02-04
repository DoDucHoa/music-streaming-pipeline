# Apache Airflow - Orchestration Layer
## Music Streaming Pipeline - Phase 6

Complete orchestration solution for automating dbt runs, data quality checks, and workflow management.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [DAGs](#dags)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

The Airflow orchestration layer automates and schedules critical data pipeline tasks:

- **Daily dbt Runs**: Full refresh of all analytics models at 2:00 AM
- **Hourly Incremental Updates**: Refresh fact and aggregation tables every hour
- **Data Quality Testing**: Automated dbt test suite every 6 hours
- **Email Notifications**: Alerts on DAG failures for quick response

### Key Features

✅ **LocalExecutor** with PostgreSQL backend for reliable task execution  
✅ **3 Production DAGs** with layered task dependencies  
✅ **Email Alerts** via SMTP for failure notifications  
✅ **Docker Integration** - DAGs control dbt via docker compose  
✅ **Web UI** on http://localhost:8080 for monitoring  
✅ **Health Checks** for all services  

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Airflow Webserver                        │
│                    (http://localhost:8080)                   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┼────────────────────────────────────┐
│                  Airflow Scheduler                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ DAG 1: dbt_daily_run (2:00 AM)                       │   │
│  │  └─ staging → intermediate → core → dims → analytics │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │ DAG 2: dbt_incremental_hourly (Every hour)           │   │
│  │  └─ fct_song_plays → fct_sessions → agg_* models    │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │ DAG 3: dbt_test_suite (Every 6 hours)                │   │
│  │  └─ All tests + source freshness + custom checks    │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│              PostgreSQL (Airflow Metadata DB)                │
└──────────────────────────────────────────────────────────────┘
                         │
                         ├──> dbt Container (docker compose)
                         ├──> BigQuery (via GCP credentials)
                         └──> SMTP Server (email notifications)
```

---

## 🧩 Components

### 1. Airflow Services

#### **airflow-postgres**
- PostgreSQL 15 database for Airflow metadata
- Stores DAG run history, task states, connections
- Port: 5432
- Health checks every 10 seconds

#### **airflow-webserver**
- Web UI for monitoring and managing DAGs
- Port: 8080
- Username: `admin` (from `.env`)
- Access: http://localhost:8080

#### **airflow-scheduler**
- Executes DAG tasks on schedule
- LocalExecutor for parallel task execution
- Scans DAGs folder every 30 seconds

#### **airflow-init**
- One-time initialization service
- Creates database schema
- Creates admin user
- Runs automatically on first `docker compose up`

### 2. Volumes

- `airflow-postgres-data`: Persistent metadata storage
- `./airflow/dags`: DAG definitions (mounted read-only)
- `./airflow/logs`: Task execution logs
- `./airflow/plugins`: Custom plugins (optional)
- `./dbt`: dbt project (for DAGs to execute dbt commands)
- `/var/run/docker.sock`: Docker socket for controlling dbt container

---

## 📅 DAGs

### DAG 1: `dbt_daily_run`

**Schedule**: Daily at 2:00 AM (`0 2 * * *`)  
**Purpose**: Full refresh of all dbt models  
**Duration**: ~20-30 minutes

**Tasks**:
1. ✅ `check_dbt_health` - Verify dbt container
2. ✅ `dbt_debug` - Test BigQuery connection
3. ✅ `dbt_models` (Task Group):
   - `run_staging_models`
   - `run_intermediate_models`
   - `run_core_models` (facts)
   - `run_dimension_models`
   - `run_analytics_models` (aggregations)
4. ✅ `dbt_test_all` - Run all 145 tests
5. ✅ `dbt_docs_generate` - Update documentation
6. ✅ `send_success_email` - Notify on completion

**Email Notifications**:
- ✅ Success summary with duration
- ❌ Failure alerts with error details

---

### DAG 2: `dbt_incremental_hourly`

**Schedule**: Every hour (`0 * * * *`)  
**Purpose**: Refresh incremental fact and aggregation tables  
**Duration**: ~5-10 minutes

**Tasks**:
1. ✅ `check_source_freshness` - Verify raw data is recent
2. ✅ `refresh_incremental_models` (Task Group):
   - `refresh_fct_song_plays` (parallel)
   - `refresh_fct_sessions` (parallel)
   - `refresh_agg_daily_metrics` (after facts)
   - `refresh_agg_hourly_activity` (after facts)
3. ✅ `test_incremental_models` - Validate updated tables

**Email Notifications**:
- ❌ Failure alerts only (to reduce noise)

---

### DAG 3: `dbt_test_suite`

**Schedule**: Every 6 hours (`0 */6 * * *`)  
**Purpose**: Comprehensive data quality validation  
**Duration**: ~10-15 minutes

**Tasks**:
1. ✅ `test_by_layer` (Task Group):
   - `test_staging_models`
   - `test_intermediate_models`
   - `test_core_models`
   - `test_dimension_models`
   - `test_analytics_models`
2. ✅ `test_custom_sql` - Run custom SQL tests
3. ✅ `check_source_freshness` - Data freshness check
4. ✅ `generate_test_report` - Summary report
5. ✅ `send_test_summary` - Email results

**Email Notifications**:
- ✅ Full test summary with pass rate
- ❌ Failure alerts with failed test details

---

## 🚀 Setup & Installation

### Prerequisites

- Docker and Docker Compose installed
- GCP credentials configured
- dbt project set up (Phase 5 complete)

### Step 1: Configure SMTP Email

Edit `.env` file with your email settings:

```bash
# For Gmail (recommended for testing):
AIRFLOW_ALERT_EMAIL=your-email@example.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_MAIL_FROM=your-airflow-email@gmail.com
SMTP_USER=your-airflow-email@gmail.com
SMTP_PASSWORD=your-gmail-app-password
```

**Gmail App Password Setup**:
1. Enable 2FA: https://myaccount.google.com/security
2. Generate App Password: https://support.google.com/accounts/answer/185833
3. Copy 16-character password to `SMTP_PASSWORD`

### Step 2: Build Airflow Image

```powershell
# Build Airflow container
docker compose build airflow-webserver airflow-scheduler airflow-init
```

### Step 3: Initialize Airflow

```powershell
# Start PostgreSQL and run initialization
docker compose up -d airflow-postgres
Start-Sleep -Seconds 10

# Run initialization (creates admin user)
docker compose up airflow-init

# Wait for "Airflow initialization complete!" message
```

### Step 4: Start Airflow Services

```powershell
# Start webserver and scheduler
docker compose up -d airflow-webserver airflow-scheduler

# Check status
docker compose ps

# View logs
docker logs airflow-scheduler -f
```

### Step 5: Access Airflow UI

1. Open browser: http://localhost:8080
2. Login:
   - **Username**: `admin`
   - **Password**: `admin` (or from `AIRFLOW_ADMIN_PASSWORD` in `.env`)
3. Verify 3 DAGs are visible:
   - `dbt_daily_run`
   - `dbt_incremental_hourly`
   - `dbt_test_suite`

---

## ⚙️ Configuration

### Environment Variables

All configuration in `.env` file:

```bash
# Airflow Database
AIRFLOW_DB_PASSWORD=airflow

# Security Keys
AIRFLOW_FERNET_KEY=<generated-key>
AIRFLOW_SECRET_KEY=airflowsecretkey

# Admin User
AIRFLOW_ADMIN_PASSWORD=admin

# Email Alerts
AIRFLOW_ALERT_EMAIL=your-email@example.com

# SMTP Settings
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
```

### Generate New Fernet Key

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### DAG Configuration

Edit DAG files in `airflow/dags/` to customize:
- Schedule intervals
- Retry policies
- Timeout settings
- Email recipients

---

## 📖 Usage

### Manual DAG Triggers

```powershell
# Trigger DAG from Web UI:
# 1. Navigate to DAG page
# 2. Click "Play" button
# 3. Select "Trigger DAG"

# Trigger from CLI:
docker exec airflow-scheduler airflow dags trigger dbt_daily_run
docker exec airflow-scheduler airflow dags trigger dbt_incremental_hourly
docker exec airflow-scheduler airflow dags trigger dbt_test_suite
```

### Enable/Disable DAGs

```powershell
# Pause DAG (stops scheduled runs)
docker exec airflow-scheduler airflow dags pause dbt_incremental_hourly

# Unpause DAG (resume scheduled runs)
docker exec airflow-scheduler airflow dags unpause dbt_incremental_hourly
```

### View Logs

```powershell
# Scheduler logs
docker logs airflow-scheduler -f

# Webserver logs
docker logs airflow-webserver -f

# Task logs (in Web UI):
# DAG page → Click task → View Log
```

### Backfill Historical Runs

```powershell
# Backfill dbt_daily_run for last 7 days
docker exec airflow-scheduler airflow dags backfill \
  dbt_daily_run \
  --start-date 2026-01-27 \
  --end-date 2026-02-03
```

---

## 📊 Monitoring

### Web UI Dashboards

1. **DAGs Page** (http://localhost:8080/home)
   - View all DAGs and their schedules
   - Recent runs and success rates
   - Quick trigger/pause actions

2. **DAG Details** (http://localhost:8080/dags/{dag_id}/grid)
   - Grid view of task runs
   - Tree view of task dependencies
   - Gantt chart for performance analysis

3. **Task Logs**
   - Click any task → "Log" button
   - Real-time streaming logs
   - Download logs for troubleshooting

### Email Notifications

Receive emails for:
- ❌ **DAG failures** (any task fails)
- ✅ **Daily run completion** (`dbt_daily_run`)
- ✅ **Test suite results** (`dbt_test_suite`)

### Health Checks

All services have health checks:
```powershell
# Check service health
docker compose ps

# Should show "healthy" status:
# airflow-postgres: healthy
# airflow-webserver: healthy
# airflow-scheduler: healthy
```

---

## 🐛 Troubleshooting

### Issue: Airflow UI not accessible

**Solution**:
```powershell
# Check webserver logs
docker logs airflow-webserver --tail 50

# Restart webserver
docker compose restart airflow-webserver

# Wait 60 seconds for startup
Start-Sleep -Seconds 60

# Verify health
docker compose ps airflow-webserver
```

### Issue: DAGs not appearing in UI

**Solution**:
```powershell
# Check DAGs folder is mounted
docker exec airflow-scheduler ls /opt/airflow/dags

# Force DAG refresh
docker exec airflow-scheduler airflow dags list

# Check for syntax errors
docker exec airflow-scheduler python /opt/airflow/dags/dbt_daily_run.py
```

### Issue: Email notifications not working

**Solution**:
```powershell
# Test SMTP connection
docker exec airflow-scheduler airflow tasks test dbt_daily_run send_success_email 2026-02-03

# Check SMTP credentials in .env
cat .env | Select-String "SMTP"

# Verify email settings
docker exec airflow-scheduler airflow config get-value smtp smtp_host
```

### Issue: dbt commands fail in DAGs

**Solution**:
```powershell
# Verify dbt container is accessible
docker compose ps dbt

# Test dbt command manually
docker exec airflow-scheduler docker compose -f /opt/airflow/dbt/docker-compose.yml run --rm dbt debug

# Check GCP credentials are mounted
docker exec airflow-scheduler ls /credentials/gcp-service-account-key.json
```

### Issue: Database connection errors

**Solution**:
```powershell
# Check PostgreSQL is running
docker compose ps airflow-postgres

# Verify connection string
docker exec airflow-scheduler airflow config get-value database sql_alchemy_conn

# Reset database (WARNING: deletes all run history)
docker compose down
docker volume rm music-streaming-pipeline_airflow-postgres-data
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

### Common Log Messages

**Normal**:
```
INFO - Marking task as SUCCESS
INFO - Task exited with return code 0
```

**Warning (non-critical)**:
```
WARNING - DAG dbt_daily_run is paused at creation
```

**Error (needs attention)**:
```
ERROR - Task failed with exception
ERROR - SMTP connection failed
```

---

## 📚 Additional Resources

### Documentation
- [Airflow Official Docs](https://airflow.apache.org/docs/)
- [dbt DAG Best Practices](https://docs.getdbt.com/docs/deploy/airflow)
- [Project Phase 6 Progress](../../docs/personal-local-docs/PROJECT_PROGRESS.md)

### Related Files
- `docker-compose.yml` - Service definitions
- `.env` - Configuration variables
- `dbt/` - dbt project (orchestrated by DAGs)
- `airflow/dags/` - DAG definitions

### Monitoring Tools
- **Airflow UI**: http://localhost:8080
- **Spark UI**: http://localhost:4040 (when Spark is running)
- **BigQuery Console**: https://console.cloud.google.com/bigquery

---

## 🎓 Academic Project Notes

This Airflow implementation demonstrates:
- ✅ **Workflow Orchestration** - Automated scheduling of data pipelines
- ✅ **Data Quality Management** - Scheduled testing and validation
- ✅ **Monitoring & Alerting** - Email notifications for failures
- ✅ **Docker Integration** - Container-based workflow execution
- ✅ **Production Patterns** - Retries, timeouts, health checks

Perfect for showcasing end-to-end data engineering skills in portfolio or academic submissions.

---

**Phase 6 Status**: ✅ **COMPLETE**  
**Implementation Date**: February 3, 2026  
**Maintained by**: Data Engineering Team
