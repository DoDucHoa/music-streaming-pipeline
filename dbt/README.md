# 📊 dbt Analytics Layer

This directory contains the dbt (data build tool) project for transforming raw streaming events into analytics-ready tables.

## 📁 Project Structure

```text
dbt/
├── models/
│   ├── staging/               # Clean and standardize raw data
│   │   ├── stg_events.sql
│   │   ├── stg_song_plays.sql
│   │   └── _staging.yml
│   ├── intermediate/          # Business logic transformations
│   │   ├── int_user_sessions.sql
│   │   ├── int_song_enriched.sql
│   │   └── _intermediate.yml
│   └── marts/                 # Analytics-ready tables
│       ├── core/              # Core fact and dimension tables
│       │   ├── fct_song_plays.sql
│       │   ├── fct_sessions.sql
│       │   └── _core.yml
│       ├── dimensions/        # Dimension tables
│       │   ├── dim_users.sql
│       │   ├── dim_songs.sql
│       │   └── _dimensions.yml
│       └── analytics/         # Aggregated metrics
│           ├── agg_daily_metrics.sql
│           ├── agg_hourly_activity.sql
│           ├── agg_user_summary.sql
│           └── _analytics.yml
├── macros/                    # Custom SQL macros
├── tests/                     # Custom data tests
├── seeds/                     # Static reference data
├── snapshots/                 # SCD Type 2 snapshots
├── dbt_project.yml           # Project configuration
├── profiles.yml              # BigQuery connection
└── packages.yml              # External packages

```

## 🎯 Data Flow

```text
Raw Data (Spark)          Staging              Intermediate              Marts
─────────────────         ───────              ────────────              ─────

raw_events          →     stg_events      →   int_user_sessions    →   fct_sessions
  (BigQuery)              (view)              (ephemeral)              (table)
                          
                          stg_song_plays  →   int_song_enriched    →   fct_song_plays
                          (view)              (ephemeral)              (incremental)
                          
                                                                    →   dim_users
                                                                        (table)
                                                                    
                                                                    →   dim_songs
                                                                        (table)
                                                                    
                                                                    →   agg_daily_metrics
                                                                        (incremental)
                                                                    
                                                                    →   agg_hourly_activity
                                                                        (incremental)
                                                                    
                                                                    →   agg_user_summary
                                                                        (table)
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **GCP Service Account** with BigQuery permissions
3. **Raw data flowing** from Spark Streaming to `music_streaming_data.raw_events`

### Setup

```powershell
# 1. Navigate to dbt directory
cd dbt

# 2. Create Python virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dbt-bigquery
pip install dbt-bigquery==1.7.0

# 4. Install dbt packages
dbt deps

# 5. Test connection
dbt debug

# Expected output: "All checks passed!"
```

### Run dbt Models

```powershell
# Run all models
dbt run

# Run specific model
dbt run --select stg_events

# Run models by tag
dbt run --select tag:staging
dbt run --select tag:marts

# Run with full refresh (rebuild incremental models)
dbt run --full-refresh

# Run specific model and downstream dependencies
dbt run --select stg_events+
```

### Test Data Quality

```powershell
# Run all tests
dbt test

# Test specific model
dbt test --select stg_events

# Run tests by tag
dbt test --select tag:core
```

### Generate Documentation

```powershell
# Generate and serve documentation site
dbt docs generate
dbt docs serve

# Documentation will be available at http://localhost:8080
```

## 📊 Model Descriptions

### Staging Layer (`staging/`)

**Purpose**: Clean and standardize raw data from source systems.

- **`stg_events`**: All events with cleaned fields, consistent data types
- **`stg_song_plays`**: Filtered to only song play events (NextSong page)

**Materialization**: Views (fast, always up-to-date)

### Intermediate Layer (`intermediate/`)

**Purpose**: Apply business logic transformations, not exposed to end users.

- **`int_user_sessions`**: Sessionized user activity with session metrics
- **`int_song_enriched`**: Song plays with enriched metadata

**Materialization**: Ephemeral (inline CTEs, not materialized)

### Core Marts (`marts/core/`)

**Purpose**: Core analytical tables (facts and dimensions).

#### Fact Tables

- **`fct_song_plays`**: One row per song play with all relevant dimensions
  - Partitioned by `event_date`
  - Clustered by `user_id`, `artist`, `song`
  - Incremental (only new data processed)

- **`fct_sessions`**: One row per user session with session-level metrics
  - Partitioned by `session_date`
  - Clustered by `user_id`, `session_date`
  - Incremental

#### Dimension Tables

- **`dim_users`**: User information (latest state)
- **`dim_songs`**: Unique songs and artists

### Analytics Marts (`marts/analytics/`)

**Purpose**: Pre-aggregated metrics for dashboards and reporting.

- **`agg_daily_metrics`**: Daily platform metrics (plays, users, sessions)
  - Incremental, partitioned by date
  
- **`agg_hourly_activity`**: Hourly active users and activity
  - Incremental, partitioned by date
  
- **`agg_user_summary`**: Per-user lifetime metrics (total plays, favorite songs)
  - Full table refresh

## 🔧 Configuration

### Incremental Strategy

Incremental models use `insert_overwrite` strategy with lookback window:

```yaml
+incremental_strategy: insert_overwrite
+partition_by:
  field: event_date
  data_type: date
  granularity: day
```

**Lookback window**: 3 days (configurable in `dbt_project.yml`)

This means each run reprocesses the last 3 days to handle late-arriving data.

### BigQuery Optimization

Models are optimized for BigQuery:

- **Partitioning**: By date for efficient time-based queries
- **Clustering**: By common filter columns
- **Incremental loads**: Reduce processing time and cost
- **Byte limits**: Set in `profiles.yml` to prevent runaway queries

## 🧪 Testing Strategy

### Built-in Tests

```yaml
# Example: tests in _staging.yml
models:
  - name: stg_events
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
      - name: event_timestamp
        tests:
          - not_null
```

### Custom Tests

Located in `tests/` directory for complex business logic validation.

## 🐳 Docker Integration

### Run dbt in Docker

```powershell
# Build dbt Docker image
docker build -t dbt-music-streaming:latest -f docker/Dockerfile .

# Run dbt commands
docker run --rm \
  -v ${PWD}:/usr/app \
  -v ${PWD}/../spark-streaming/credentials:/credentials:ro \
  dbt-music-streaming:latest \
  dbt run

# Run with docker-compose
docker-compose run --rm dbt run
```

## 📈 Usage Examples

### Daily Analytics Refresh

```powershell
# Run all incremental models for today
dbt run --select tag:marts

# This will:
# 1. Process last 3 days of data (lookback window)
# 2. Update fact tables with new events
# 3. Refresh aggregation tables
```

### Full Rebuild

```powershell
# Rebuild everything from scratch
dbt run --full-refresh

# Warning: This can be expensive on large datasets!
```

### Specific Model Refresh

```powershell
# Update only daily metrics
dbt run --select agg_daily_metrics

# Update song plays and all downstream models
dbt run --select fct_song_plays+
```

## 📊 Key Metrics Available

### Daily Metrics (`agg_daily_metrics`)

- Total song plays
- Unique users
- Total sessions
- Average session duration
- Most played songs
- Most active users

### Hourly Activity (`agg_hourly_activity`)

- Active users per hour
- Song plays per hour
- Peak usage times

### User Summary (`agg_user_summary`)

- User lifetime value metrics
- Total plays per user
- Favorite artists/songs
- User subscription level history

## 🔍 Troubleshooting

### Connection Issues

```powershell
# Test BigQuery connection
dbt debug

# Common fixes:
# 1. Check keyfile path in profiles.yml
# 2. Verify service account has BigQuery permissions
# 3. Ensure dataset exists (music_streaming_analytics)
```

### Model Failures

```powershell
# Run with verbose logging
dbt run --debug

# View compiled SQL
cat target/compiled/music_streaming_analytics/models/path/to/model.sql
```

### Performance Issues

```powershell
# Check query performance in BigQuery Console
# Look for:
# - Full table scans (should use partitions/clusters)
# - Large shuffles
# - Expensive joins

# Profile model execution
dbt run --select my_model --profiles-dir .
```

## 📚 Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [BigQuery Optimization](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
- [dbt Discourse](https://discourse.getdbt.com/)

## 🤝 Contributing

When adding new models:

1. Place in appropriate layer (staging/intermediate/marts)
2. Add documentation in corresponding `_*.yml` file
3. Add data tests for data quality
4. Update this README with model description
5. Consider partitioning/clustering for large tables

---

**Project**: Music Streaming Analytics Pipeline  
**dbt Version**: 1.7.0  
**Database**: Google BigQuery  
**Last Updated**: February 2, 2026
