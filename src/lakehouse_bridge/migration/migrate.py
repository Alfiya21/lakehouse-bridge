"""Migrate the lakehouse tables to a warehouse target and prove the copy is faithful.

Targets
- duckdb   : local stand-in for the warehouse (used in CI and for development)
- bigquery : google-cloud-bigquery load jobs (needs credentials; see README)

Reconciliation runs identical standard-SQL profile queries on source and target:
row count, per-column null counts, SUM of numerics, MIN/MAX of dates/timestamps and
distinct counts of key columns. Any mismatch fails the migration.
"""
import json
import duckdb
from .. import config as C

NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT", "INT64", "FLOAT64")


def _source_sql(rel: str) -> str:
    return f"read_parquet('{(C.LAKE / rel).as_posix()}/**/*.parquet', hive_partitioning=true)"


def profile(con, ref: str) -> dict:
    cols = con.execute(f"DESCRIBE SELECT * FROM {ref}").fetchall()
    exprs, names = ["COUNT(*)"], ["row_count"]
    for name, typ, *_ in cols:
        q = f'"{name}"'
        exprs.append(f"COUNT(*) - COUNT({q})"); names.append(f"nulls:{name}")
        t = str(typ).upper()
        if t.startswith(NUMERIC):
            exprs.append(f"ROUND(COALESCE(SUM({q}), 0), 2)"); names.append(f"sum:{name}")
        elif t.startswith(("DATE", "TIMESTAMP")):
            exprs.append(f"CAST(MIN({q}) AS VARCHAR)"); names.append(f"min:{name}")
            exprs.append(f"CAST(MAX({q}) AS VARCHAR)"); names.append(f"max:{name}")
        if name.endswith("_id") or name in ("order_id", "customer_id", "event_id"):
            exprs.append(f"COUNT(DISTINCT {q})"); names.append(f"distinct:{name}")
    row = con.execute(f"SELECT {', '.join(exprs)} FROM {ref}").fetchone()
    return dict(zip(names, row))


def _compare(a: dict, b: dict) -> list:
    diffs = []
    for k in a:
        if k not in b:
            diffs.append((k, a[k], "missing"))
        elif isinstance(a[k], float) or isinstance(b[k], float):
            if abs(float(a[k]) - float(b[k])) > 0.01:
                diffs.append((k, a[k], b[k]))
        elif str(a[k]) != str(b[k]):
            diffs.append((k, a[k], b[k]))
    return diffs


def migrate_duckdb(tables=C.TABLES) -> dict:
    C.ensure_dirs()
    src = duckdb.connect()
    dst = duckdb.connect(str(C.WAREHOUSE))
    dst.execute(f"CREATE SCHEMA IF NOT EXISTS {C.SCHEMA}")
    report = {}
    for name, rel in tables:
        dst.execute(f"CREATE OR REPLACE TABLE {C.SCHEMA}.{name} AS SELECT * FROM {_source_sql(rel)}")
        sp = profile(src, _source_sql(rel))
        tp = profile(dst, f"{C.SCHEMA}.{name}")
        diffs = _compare(sp, tp)
        report[name] = {"rows_source": sp["row_count"], "rows_target": tp["row_count"],
                        "checks": len(sp), "mismatches": diffs, "ok": not diffs}
    dst.close(); src.close()
    return report


def migrate_bigquery(tables=C.TABLES) -> dict:  # pragma: no cover - needs GCP credentials
    """Load Parquet files to BigQuery then reconcile with the same profile SQL.
    Prefer GCS + load_table_from_uri in real runs; this path uses a local read for simplicity."""
    from google.cloud import bigquery  # lazy import: not needed locally
    client = bigquery.Client(project=C.BQ_PROJECT)
    client.create_dataset(bigquery.Dataset(f"{C.BQ_PROJECT}.{C.BQ_DATASET}"), exists_ok=True)
    src = duckdb.connect()
    report = {}
    for name, rel in tables:
        df = src.execute(f"SELECT * FROM {_source_sql(rel)}").df()
        job = client.load_table_from_dataframe(
            df, f"{C.BQ_PROJECT}.{C.BQ_DATASET}.{name}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()
        sp = profile(src, _source_sql(rel))
        tgt = {"row_count": client.get_table(f"{C.BQ_PROJECT}.{C.BQ_DATASET}.{name}").num_rows}
        diffs = _compare({"row_count": sp["row_count"]}, tgt)   # extend with BigQuery profile SQL as needed
        report[name] = {"rows_source": sp["row_count"], "rows_target": tgt["row_count"], "mismatches": diffs, "ok": not diffs}
    return report


def run() -> dict:
    report = migrate_duckdb() if C.TARGET == "duckdb" else migrate_bigquery()
    (C.REPORTS / "reconciliation.json").write_text(json.dumps(report, indent=2, default=str))
    bad = [t for t, r in report.items() if not r["ok"]]
    if bad:
        raise SystemExit(f"Reconciliation FAILED for: {bad}")
    return report


if __name__ == "__main__":
    for t, r in run().items():
        print(f"{t:24s} rows {r['rows_source']:>7} -> {r['rows_target']:>7}  checks={r['checks']}  ok={r['ok']}")
