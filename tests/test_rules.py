"""Tests for daggate. Run: pytest tests/ -v"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import daggate  # noqa: E402


def run_src(tmp_path, src: str):
    f = tmp_path / "dag_under_test.py"
    f.write_text(src)
    findings, errors = daggate.check_path(f)
    assert not errors, errors
    return {fi.code for fi in findings}, findings


MINIMAL_DAG = """
from airflow import DAG
dag = DAG(dag_id="x", {args})
"""

FULL_ARGS = (
    'schedule="@daily", catchup=False, tags=["t"], doc_md="d", '
    "dagrun_timeout=__import__('datetime').timedelta(hours=1), "
    "default_args={'owner': 'me', 'retries': 2, "
    "'retry_delay': __import__('datetime').timedelta(minutes=5), "
    "'on_failure_callback': print}"
)


def full_dag(remove: str = "", extra: str = "") -> str:
    args = FULL_ARGS
    if remove:
        # crude but effective for tests: drop a kwarg or dict key by name
        import re
        args = re.sub(rf"'{remove}':[^,}}]+,?\s*", "", args)
        args = re.sub(rf"{remove}=[^,]+,\s*", "", args)
    if extra:
        args += ", " + extra
    return MINIMAL_DAG.format(args=args)


def test_clean_dag_passes(tmp_path):
    codes, _ = run_src(tmp_path, full_dag())
    assert codes == set()


def test_dg101_catchup(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="catchup"))
    assert "DG101" in codes


def test_dg102_retries(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="retries"))
    assert "DG102" in codes


def test_dg103_retry_delay(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="retry_delay"))
    assert "DG103" in codes


def test_dg104_owner(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="owner"))
    assert "DG104" in codes


def test_dg105_tags(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="tags"))
    assert "DG105" in codes


def test_dg106_doc(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="doc_md"))
    assert "DG106" in codes


def test_dg106_description_suffices(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="doc_md", extra='description="ok"'))
    assert "DG106" not in codes


def test_dg107_schedule(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="schedule"))
    assert "DG107" in codes


def test_dg107_schedule_none_is_explicit(tmp_path):
    src = full_dag(remove="schedule", extra="schedule=None")
    codes, _ = run_src(tmp_path, src)
    assert "DG107" not in codes


def test_dg201_variable_get_top_level(tmp_path):
    src = "from airflow.models import Variable\nENV = Variable.get('env')\n"
    codes, _ = run_src(tmp_path, src)
    assert "DG201" in codes


def test_dg201_ok_inside_function(tmp_path):
    src = (
        "from airflow.models import Variable\n"
        "def f():\n    return Variable.get('env')\n"
    )
    codes, _ = run_src(tmp_path, src)
    assert "DG201" not in codes


def test_dg202_requests_top_level(tmp_path):
    src = "import requests\nCFG = requests.get('https://x.test').json()\n"
    codes, _ = run_src(tmp_path, src)
    assert "DG202" in codes


def test_dg202_pandas_read_top_level(tmp_path):
    src = "import pandas as pd\ndf = pd.read_csv('big.csv')\n"
    codes, _ = run_src(tmp_path, src)
    assert "DG202" in codes


def test_dg202_ok_inside_task_fn(tmp_path):
    src = "import requests\ndef task():\n    return requests.get('https://x.test')\n"
    codes, _ = run_src(tmp_path, src)
    assert "DG202" not in codes


def test_dg203_dynamic_start_date(tmp_path):
    src = full_dag() .replace(
        'dag = DAG(dag_id="x"',
        'from datetime import datetime\ndag = DAG(dag_id="x", '
        "start_date=datetime.now()",
    )
    codes, _ = run_src(tmp_path, src)
    assert "DG203" in codes


def test_dg204_naive_start_date(tmp_path):
    src = full_dag().replace(
        'dag = DAG(dag_id="x"',
        'from datetime import datetime\ndag = DAG(dag_id="x", '
        "start_date=datetime(2026, 1, 1)",
    )
    codes, _ = run_src(tmp_path, src)
    assert "DG204" in codes


def test_dg204_pendulum_ok(tmp_path):
    src = full_dag().replace(
        'dag = DAG(dag_id="x"',
        'import pendulum\ndag = DAG(dag_id="x", '
        "start_date=pendulum.datetime(2026, 1, 1, tz='UTC')",
    )
    codes, _ = run_src(tmp_path, src)
    assert "DG204" not in codes and "DG203" not in codes


def test_dg301_hardcoded_secret(tmp_path):
    codes, _ = run_src(tmp_path, "db_password = 'hunter2'\n")
    assert "DG301" in codes


def test_dg301_placeholder_ok(tmp_path):
    codes, _ = run_src(tmp_path, "db_password = '{{ var.value.pw }}'\n")
    assert "DG301" not in codes


def test_dg302_cred_url(tmp_path):
    codes, _ = run_src(tmp_path, "URL = 'postgresql://u:pw@h:5432/db'\n")
    assert "DG302" in codes


def test_dg401_dagrun_timeout(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="dagrun_timeout"))
    assert "DG401" in codes


def test_dg402_alerting(tmp_path):
    codes, _ = run_src(tmp_path, full_dag(remove="on_failure_callback"))
    assert "DG402" in codes


def test_dg402_email_on_failure_suffices(tmp_path):
    src = full_dag(remove="on_failure_callback")
    src = src.replace("'owner': 'me'", "'owner': 'me', 'email_on_failure': True")
    codes, _ = run_src(tmp_path, src)
    assert "DG402" not in codes


def test_dg403_depends_on_past(tmp_path):
    src = full_dag()
    src = src.replace("'owner': 'me'", "'owner': 'me', 'depends_on_past': True")
    codes, _ = run_src(tmp_path, src)
    assert "DG403" in codes
    src2 = src.replace('dag = DAG(dag_id="x"', 'dag = DAG(dag_id="x", max_active_runs=1')
    codes2, _ = run_src(tmp_path, src2)
    assert "DG403" not in codes2


def test_default_args_defined_separately(tmp_path):
    src = (
        "from airflow import DAG\n"
        "import datetime\n"
        "default_args = {'owner': 'me', 'retries': 1,\n"
        " 'retry_delay': datetime.timedelta(minutes=1),\n"
        " 'on_failure_callback': print}\n"
        'dag = DAG(dag_id="x", schedule=None, catchup=False, tags=["t"],\n'
        ' doc_md="d", dagrun_timeout=datetime.timedelta(hours=1),\n'
        " default_args=default_args)\n"
    )
    codes, _ = run_src(tmp_path, src)
    assert codes == set()


def test_taskflow_dag_decorator(tmp_path):
    src = (
        "from airflow.decorators import dag\n"
        "@dag(dag_id='x')\n"
        "def my_flow():\n    pass\n"
        "my_flow()\n"
    )
    codes, _ = run_src(tmp_path, src)
    assert "DG101" in codes and "DG107" in codes


def test_cli_exit_codes(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text("from airflow import DAG\ndag = DAG(dag_id='x')\n")
    assert daggate.main([str(bad)]) == 1
    good = tmp_path / "good.py"
    good.write_text(full_dag())
    assert daggate.main([str(good)]) == 0


def test_cli_select_ignore(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text("from airflow import DAG\ndag = DAG(dag_id='x')\n")
    # only warnings selected -> exit 0 without --strict
    assert daggate.main([str(bad), "--select", "DG105"]) == 0
    assert daggate.main([str(bad), "--select", "DG105", "--strict"]) == 1


def test_example_fixtures():
    root = Path(__file__).resolve().parent.parent / "examples" / "dags"
    bad_findings, _ = daggate.check_path(root / "bad_etl_dag.py")
    bad_codes = {f.code for f in bad_findings}
    expected = {"DG101", "DG102", "DG104", "DG105", "DG106", "DG107",
                "DG201", "DG202", "DG203", "DG301", "DG302", "DG401", "DG402"}
    assert expected <= bad_codes, expected - bad_codes
    good_findings, _ = daggate.check_path(root / "good_etl_dag.py")
    assert good_findings == []
