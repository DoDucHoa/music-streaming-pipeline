"""
dbt Incremental Hourly Refresh DAG
Music Streaming Pipeline - Phase 6 Orchestration

Refreshes incremental fact and aggregation tables every hour
Optimized for near-real-time analytics updates
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

# Helper function to wrap docker-compose commands  
# CRITICAL: Working directory MUST be /opt/airflow (not /opt/airflow/dbt) because
# docker-compose resolves volume paths relative to working directory on HOST machine.
# /opt/airflow in container maps to project root on HOST.
def dbt_cmd(command: str) -> str:
    """Wrap docker-compose dbt command - working directory is /opt/airflow"""
    return f'cd /opt/airflow && docker compose -f dbt/docker-compose.yml run --rm dbt {command}'

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['${AIRFLOW_ALERT_EMAIL}'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),
}

# DAG definition
with DAG(
    dag_id='dbt_incremental_hourly',
    default_args=default_args,
    description='Refresh incremental dbt models every hour',
    schedule_interval='0 * * * *',  # Every hour at :00
    start_date=datetime(2026, 2, 3),
    catchup=False,
    tags=['dbt', 'hourly', 'incremental', 'analytics'],
    max_active_runs=1,
) as dag:

    # Task 1: Check source data freshness
    check_freshness = BashOperator(
        task_id='check_source_freshness',
        bash_command=dbt_cmd('source freshness'),
        cwd='/opt/airflow',
    )

    # Task Group: Refresh incremental models
    with TaskGroup('refresh_incremental_models') as refresh_models:
        
        # Fact tables (incremental)
        refresh_fact_song_plays = BashOperator(
            task_id='refresh_fct_song_plays',
            bash_command=dbt_cmd('run --select fct_song_plays'),
            cwd='/opt/airflow',
        )

        refresh_fact_sessions = BashOperator(
            task_id='refresh_fct_sessions',
            bash_command=dbt_cmd('run --select fct_sessions'),
            cwd='/opt/airflow',
        )

        # Analytics aggregations (incremental)
        refresh_daily_metrics = BashOperator(
            task_id='refresh_agg_daily_metrics',
            bash_command=dbt_cmd('run --select agg_daily_metrics'),
            cwd='/opt/airflow',
        )

        refresh_hourly_activity = BashOperator(
            task_id='refresh_agg_hourly_activity',
            bash_command=dbt_cmd('run --select agg_hourly_activity'),
            cwd='/opt/airflow',
        )

        # Facts run in parallel, then aggregations
        refresh_fact_song_plays >> refresh_daily_metrics
        refresh_fact_song_plays >> refresh_hourly_activity
        refresh_fact_sessions >> refresh_daily_metrics
        refresh_fact_sessions >> refresh_hourly_activity

    # Task 2: Run tests on incremental models only
    test_incremental = BashOperator(
        task_id='test_incremental_models',
        bash_command=dbt_cmd('test --select fct_song_plays fct_sessions agg_daily_metrics agg_hourly_activity'),
        cwd='/opt/airflow',
    )

    # Task 3: Success notification (only for failures to reduce noise)
    # Email only sent if tasks fail (handled by default_args)

    # Define task dependencies
    check_freshness >> refresh_models >> test_incremental
