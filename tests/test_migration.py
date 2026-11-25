import duckdb
from lakehouse_bridge.migration import migrate


def test_all_tables_reconcile(pipeline):
    assert all(r["ok"] for r in pipeline["recon"].values())
    assert all(r["checks"] >= 10 for r in pipeline["recon"].values())


def test_reconciliation_detects_drift():
    con = duckdb.connect()
    con.execute("CREATE TABLE a AS SELECT range AS order_id, range * 1.5 AS amount FROM range(100)")
    con.execute("CREATE TABLE b AS SELECT * FROM a WHERE order_id <> 7")           # one row lost
    diffs = migrate._compare(migrate.profile(con, "a"), migrate.profile(con, "b"))
    keys = {d[0] for d in diffs}
    assert "row_count" in keys and "sum:amount" in keys
