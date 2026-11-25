import duckdb
from lakehouse_bridge import config as C
from lakehouse_bridge.governance import security, catalog


def _con(role):
    return duckdb.connect(str(security.role_db_path(role)), read_only=True)


def test_pii_is_detected(pipeline):
    cat = catalog.build()
    assert set(cat["tables"]["silver_customers"]["pii_columns"]) == {"full_name", "email", "phone"}
    assert cat["tables"]["gold_daily_revenue"]["pii_columns"] == []


def test_row_level_security(pipeline):
    assert _con("analyst_apac").execute("SELECT DISTINCT region FROM analytics.silver_customers").fetchall() == [("APAC",)]
    assert _con("analyst_apac").execute("SELECT DISTINCT region FROM analytics.silver_orders").fetchall() == [("APAC",)]
    assert len(_con("analyst_global").execute("SELECT DISTINCT region FROM analytics.silver_customers").fetchall()) == 3


def test_column_masking_and_admin_raw(pipeline):
    masked = _con("analyst_apac").execute("SELECT email, phone, full_name FROM analytics.silver_customers LIMIT 1").fetchone()
    assert masked[0].startswith("***@") and masked[1].startswith("***") and masked[2].endswith("***")
    raw = duckdb.connect(str(C.WAREHOUSE), read_only=True).execute("SELECT email FROM analytics.silver_customers WHERE email IS NOT NULL LIMIT 1").fetchone()[0]
    assert raw.startswith("user")


def test_restricted_table_absent_for_analysts(pipeline):
    tables = [r[0] for r in _con("analyst_apac").execute("SELECT table_name FROM information_schema.tables WHERE table_schema='analytics'").fetchall()]
    assert "quarantine_orders" not in tables and "silver_orders" in tables


def test_bigquery_ddl_rendering(pipeline):
    ddl = security.bigquery_ddl("my-proj", "lb")
    assert "CREATE OR REPLACE ROW ACCESS POLICY analyst_apac_rows" in ddl
    assert "SAFE_OFFSET" in ddl and "`my-proj.lb_analyst_apac.silver_customers`" in ddl
    assert "quarantine_orders" not in ddl.split("ROW ACCESS POLICY")[0]
