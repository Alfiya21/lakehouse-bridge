import ast
from lakehouse_bridge import config as C


def test_dag_is_valid_python_with_expected_tasks():
    src = (C.ROOT / "dags" / "lakehouse_bridge_dag.py").read_text()
    ast.parse(src)
    for t in ("generate_landing_data", "spark_batch_medallion", "spark_streaming_payments", "migrate_and_reconcile",
              "quality_gate", "catalog_and_security", "dbt_build", "agent_evaluation"):
        assert f'task_id="{t}"' in src
    assert "gen >> [batch, stream] >> migrate >> quality >> govern >> dbt >> agent_eval" in src
