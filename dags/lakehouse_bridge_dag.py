"""Airflow / Cloud Composer DAG: ingest -> transform -> quality gate -> migrate -> reconcile -> govern -> dbt -> agent eval.

Not executed in CI (Airflow is not installed there); `tests/test_dag.py` compiles it and checks the task graph
statically. On Cloud Composer, replace the BashOperators for Spark steps with DataprocCreateBatchOperator.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PY = "cd /home/airflow/gcs/data/lakehouse-bridge && PYTHONPATH=src python -m"

with DAG("lakehouse_bridge", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False,
         default_args={"retries": 1}, tags=["lakehouse", "migration", "genai"]) as dag:
    gen = BashOperator(task_id="generate_landing_data", bash_command=f"{PY} lakehouse_bridge.data_gen")
    batch = BashOperator(task_id="spark_batch_medallion", bash_command=f"{PY} lakehouse_bridge.pipelines.batch")
    stream = BashOperator(task_id="spark_streaming_payments", bash_command=f"{PY} lakehouse_bridge.pipelines.streaming")
    migrate = BashOperator(task_id="migrate_and_reconcile", bash_command=f"{PY} lakehouse_bridge.migration.migrate")
    quality = BashOperator(task_id="quality_gate", bash_command=f"{PY} lakehouse_bridge.governance.quality")
    govern = BashOperator(task_id="catalog_and_security", bash_command=f"{PY} lakehouse_bridge.governance.security")
    dbt = BashOperator(task_id="dbt_build", bash_command="cd /home/airflow/gcs/data/lakehouse-bridge/dbt_project && dbt build --profiles-dir . --target ${DBT_TARGET:-bigquery}")
    agent_eval = BashOperator(task_id="agent_evaluation", bash_command=f"{PY} lakehouse_bridge.agent.eval")

    gen >> [batch, stream] >> migrate >> quality >> govern >> dbt >> agent_eval
