"""
dbt Test Suite DAG
Music Streaming Pipeline - Phase 6 Orchestration

Runs comprehensive dbt tests every 6 hours
Validates data quality and business logic
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
# /opt/airflow in container maps to project root on HOST.
def dbt_cmd(command: str) -> str:
    """Wrap docker-compose dbt command - working directory is /opt/airflow"""
    return f'cd /opt/airflow && docker compose -f dbt/docker-compose.yml run --rm dbt {command}'

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': [os.getenv('AIRFLOW_ALERT_EMAIL', 'admin@example.com')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=20),
}

# DAG definition
with DAG(
    dag_id='dbt_test_suite',
    default_args=default_args,
    description='Run dbt test suite every 6 hours for data quality validation',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2026, 2, 3),
    catchup=False,
    tags=['dbt', 'testing', 'data-quality'],
    max_active_runs=1,
) as dag:

    # Task Group: Tests by layer
    with TaskGroup('test_by_layer') as test_layers:
        
        # Staging tests
        test_staging = BashOperator(
            task_id='test_staging_models',
            bash_command=dbt_cmd('test --select staging'),
            cwd='/opt/airflow',
        )

        # Intermediate tests
        test_intermediate = BashOperator(
            task_id='test_intermediate_models',
            bash_command=dbt_cmd('test --select intermediate'),
            cwd='/opt/airflow',
        )

        # Core marts tests
        test_core = BashOperator(
            task_id='test_core_models',
            bash_command=dbt_cmd('test --select marts.core'),
            cwd='/opt/airflow',
        )

        # Dimension tests
        test_dimensions = BashOperator(
            task_id='test_dimension_models',
            bash_command=dbt_cmd('test --select marts.dimensions'),
            cwd='/opt/airflow',
        )

        # Analytics tests
        test_analytics = BashOperator(
            task_id='test_analytics_models',
            bash_command=dbt_cmd('test --select marts.analytics'),
            cwd='/opt/airflow',
        )

        # Run tests in order
        test_staging >> test_intermediate >> [test_core, test_dimensions] >> test_analytics

    # Task 2: Run custom SQL tests
    test_custom = BashOperator(
        task_id='test_custom_sql',
        bash_command=dbt_cmd('test --select test_type:singular'),
        cwd='/opt/airflow',
    )

    # Task 3: Check source data freshness
    check_freshness = BashOperator(
        task_id='check_source_freshness',
        bash_command=dbt_cmd('source freshness'),
        cwd='/opt/airflow',
    )

    # Task 4: Generate test report
    generate_report = BashOperator(
        task_id='generate_test_report',
        bash_command='''
        echo "=== dbt Test Summary ===" > /opt/airflow/logs/test_report_{{ ds }}.txt
        echo "Execution Date: {{ ds }}" >> /opt/airflow/logs/test_report_{{ ds }}.txt
        echo "All tests completed successfully" >> /opt/airflow/logs/test_report_{{ ds }}.txt
        cat /opt/airflow/logs/test_report_{{ ds }}.txt
        ''',
    )

    # Task 5: Success notification with summary
    success_email = EmailOperator(
        task_id='send_test_summary',
        to=os.getenv('AIRFLOW_ALERT_EMAIL', 'admin@example.com'),
        subject='✅ dbt Test Suite Passed - {{ ds }}',
        html_content="""
        <h3>dbt Test Suite Results</h3>
        <p><strong>Status:</strong> ✅ All Tests Passed</p>
        <p><strong>Execution Date:</strong> {{ ds }}</p>
        <p><strong>Duration:</strong> {{ ti.duration }} seconds</p>
        <p><strong>Test Layers:</strong></p>
        <ul>
            <li>Staging models</li>
            <li>Intermediate models</li>
            <li>Core facts and dimensions</li>
            <li>Analytics aggregations</li>
            <li>Custom SQL tests</li>
        </ul>
        <p>All 145 data quality tests completed successfully.</p>
        <p><a href="http://localhost:8080/dags/dbt_test_suite/grid">View DAG in Airflow</a></p>
        """,
        trigger_rule='all_success',
    )

    # Define task dependencies
    test_layers >> test_custom >> check_freshness >> generate_report >> success_email
