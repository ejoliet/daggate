"""BAD example DAG — trips nearly every daggate rule. Do not deploy."""
from datetime import datetime

import requests  # noqa: F401
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# DG201: Variable.get at parse time
ENV = Variable.get("env")

# DG202: network call at parse time
CONFIG = requests.get("https://example.com/config").json()

# DG301: hardcoded secret
db_password = "hunter2-prod"

# DG302: credentials in URL
CONN = "postgresql://etl_user:s3cret@db.internal:5432/warehouse"

# DG101 no catchup, DG102 no retries, DG104 no owner, DG105 no tags,
# DG106 no doc, DG107 no schedule, DG203 dynamic start_date,
# DG401 no dagrun_timeout, DG402 no alerting
dag = DAG(
    dag_id="bad_etl",
    start_date=datetime.now(),
)

task = BashOperator(task_id="run", bash_command="echo hi", dag=dag)
