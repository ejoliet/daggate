"""GOOD example DAG — passes daggate clean. Use as a starting template."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


def alert_on_failure(context):
    """Send failure alert (wire to Slack/SNS/PagerDuty in your environment)."""
    # AIDEV-NOTE: keep alerting logic in a shared module in real deployments.
    print(f"ALERT: {context['dag'].dag_id} failed")


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="good_etl",
    description="Daily warehouse load: extract -> validate -> load.",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "warehouse", "daily"],
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
) as dag:
    extract = BashOperator(task_id="extract", bash_command="echo extract")
    load = BashOperator(task_id="load", bash_command="echo load")
    extract >> load
