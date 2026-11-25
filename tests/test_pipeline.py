import duckdb
from lakehouse_bridge import config as C
from lakehouse_bridge.pipelines import streaming


def test_batch_row_accounting(pipeline):
    b = pipeline["batch"]
    # every bronze order is either a duplicate, quarantined, or in silver
    assert b["bronze_orders"] == b["silver_orders"] + b["quarantine_orders"] + b["duplicates_removed"]
    assert b["quarantine_orders"] > 0 and b["duplicates_removed"] > 0


def test_streaming_dedupes_and_is_idempotent(pipeline):
    assert pipeline["gen"]["events"] > pipeline["stream_rows"]            # replays removed
    again = streaming.run()                                               # checkpoint: nothing new to process
    assert again == pipeline["stream_rows"]


def test_gold_matches_silver(pipeline):
    con = duckdb.connect(str(C.WAREHOUSE), read_only=True)
    gold = con.execute("SELECT ROUND(SUM(revenue),2) FROM analytics.gold_daily_revenue").fetchone()[0]
    silver = con.execute("SELECT ROUND(SUM(amount),2) FROM analytics.silver_orders WHERE status<>'CANCELLED'").fetchone()[0]
    assert abs(gold - silver) < 0.05
