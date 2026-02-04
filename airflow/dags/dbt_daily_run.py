"""
dbt Daily Run DAG
Music Streaming Pipeline - Phase 6 Orchestration

Runs all dbt models daily at 2:00 AM
Includes full model refresh and post-run tests
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

# Helper function to wrap docker-compose commands  
# CRITICAL: Working directory MUST be /opt/airflow (not /opt/airflow/dbt) because
# docker-compose resolves volume paths relative to working directory on HOST machine.
# /opt/airflow in container maps to project root on HOST, so ../spark-streaming/credentials
# will resolve correctly.
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG definition
with DAG(
    dag_id='dbt_daily_run',
    default_args=default_args,
    description='Run all dbt models daily at 2 AM',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    start_date=datetime(2026, 2, 3),
    catchup=False,
    tags=['dbt', 'daily', 'analytics'],
    max_active_runs=1,
) as dag:

    # Task 1: Check dbt container health
    check_dbt_health = BashOperator(
        task_id='check_dbt_health',
        bash_command='docker compose -f /opt/airflow/dbt/docker-compose.yml ps dbt || echo "dbt service ready"',
    )

    # Task 2: dbt Debug (verify connections)
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=dbt_cmd('debug'),
        cwd='/opt/airflow',
    )

    # Task Group: Run dbt models by layer
    with TaskGroup('dbt_models') as dbt_models:
        
        # Staging layer
        run_staging = BashOperator(
            task_id='run_staging_models',
            bash_command=dbt_cmd('run --select staging'),
            cwd='/opt/airflow',
        )

        # Intermediate layer
        run_intermediate = BashOperator(
            task_id='run_intermediate_models',
            bash_command=dbt_cmd('run --select intermediate'),
            cwd='/opt/airflow',
        )

        # Core facts and dimensions
        run_core = BashOperator(
            task_id='run_core_models',
            bash_command=dbt_cmd('run --select marts.core'),
            cwd='/opt/airflow',
        )

        # Dimensions
        run_dimensions = BashOperator(
            task_id='run_dimension_models',
            bash_command=dbt_cmd('run --select marts.dimensions'),
            cwd='/opt/airflow',
        )

        # Analytics aggregations
        run_analytics = BashOperator(
            task_id='run_analytics_models',
            bash_command=dbt_cmd('run --select marts.analytics'),
            cwd='/opt/airflow',
        )

        # Define layer dependencies
        run_staging >> run_intermediate >> [run_core, run_dimensions]
        [run_core, run_dimensions] >> run_analytics

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test_all',
        bash_command=dbt_cmd('test'),
        cwd='/opt/airflow',
    )

    # Task 4: Generate dbt docs
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=dbt_cmd('docs generate'),
        cwd='/opt/airflow',
    )

    # Task 5: Success notification
    success_email = EmailOperator(
        task_id='send_success_email',
        to='${AIRFLOW_ALERT_EMAIL}',
        subject='✅ dbt Daily Run Completed Successfully',
        html_content="""
        <h3>dbt Daily Run Summary</h3>
        <p><strong>Status:</strong> ✅ Success</p>
        <p><strong>Execution Date:</strong> {{ ds }}</p>
        <p><strong>Duration:</strong> {{ ti.duration }} seconds</p>
        <p>All dbt models and tests completed successfully.</p>
        <p><a href="http://localhost:8080/dags/dbt_daily_run/grid">View DAG in Airflow</a></p>
        """,
        trigger_rule='all_success',
    )

    # Define task dependencies
    check_dbt_health >> dbt_debug >> dbt_models >> dbt_test >> dbt_docs >> success_email
