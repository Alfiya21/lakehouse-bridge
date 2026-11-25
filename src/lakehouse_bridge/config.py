"""Central configuration. Everything is overridable by environment variable so the
same code runs locally (Parquet + DuckDB) and in the cloud (Delta + BigQuery)."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("LB_ROOT", Path(__file__).resolve().parents[2]))
DATA = Path(os.environ.get("LB_DATA", ROOT / "data"))
LANDING = DATA / "landing"
LAKE = DATA / "lakehouse"
REPORTS = DATA / "reports"
WAREHOUSE = DATA / "warehouse.duckdb"
CHECKPOINTS = DATA / "checkpoints"

FORMAT = os.environ.get("LB_FORMAT", "parquet")      # parquet | delta (Databricks / Dataproc with Delta jars)
TARGET = os.environ.get("LB_TARGET", "duckdb")       # duckdb (local stand-in) | bigquery
SCALE = float(os.environ.get("LB_SCALE", "1.0"))

BQ_PROJECT = os.environ.get("LB_BQ_PROJECT", "")
BQ_DATASET = os.environ.get("LB_BQ_DATASET", "lakehouse_bridge")
SCHEMA = "analytics"

TABLES = [  # logical name -> relative lakehouse path
    ("silver_customers", "silver/customers"),
    ("silver_orders", "silver/orders"),
    ("silver_payments", "silver/payments"),
    ("quarantine_orders", "quarantine/orders"),
    ("gold_daily_revenue", "gold/daily_revenue"),
    ("gold_customer_ltv", "gold/customer_ltv"),
    ("gold_payments_hourly", "gold/payments_hourly"),
]

def ensure_dirs() -> None:
    for p in (LANDING, LAKE, REPORTS, CHECKPOINTS):
        p.mkdir(parents=True, exist_ok=True)
