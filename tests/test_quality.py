import duckdb
from lakehouse_bridge import config as C
from lakehouse_bridge.governance import quality
from lakehouse_bridge.migration.migrate import _source_sql


def test_gate_passes_on_clean_pipeline(pipeline):
    q = pipeline["quality"]
    assert q["gate_passed"] and q["total_rules"] >= 14


def test_gate_fails_on_corrupted_data(pipeline):
    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA {C.SCHEMA}")
    for name, rel in C.TABLES:
        con.execute(f"CREATE TABLE {C.SCHEMA}.{name} AS SELECT * FROM {_source_sql(rel)}")
    con.execute("INSERT INTO analytics.silver_orders SELECT * FROM analytics.silver_orders LIMIT 1")   # duplicate key
    con.execute("UPDATE analytics.silver_orders SET amount = -5 WHERE order_id = (SELECT MIN(order_id) FROM analytics.silver_orders)")
    con.execute("UPDATE analytics.silver_customers SET region = 'MARS' WHERE customer_id = 1")
    rep = quality.run(con)
    failed = {(r["table"], r["rule"]) for r in rep["results"] if not r["passed"] and r["severity"] == "error"}
    assert not rep["gate_passed"]
    assert {("silver_orders", "unique"), ("silver_orders", "min"), ("silver_customers", "accepted_values")} <= failed
