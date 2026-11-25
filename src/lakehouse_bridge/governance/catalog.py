"""Data catalog: automated metadata harvesting, PII tagging, lineage and business glossary.

Output: data/reports/catalog.json (machine readable) and catalog.md (human + RAG readable).
"""
import json
import re
import duckdb
import yaml
from .. import config as C

PII_NAME_HINTS = {"email": "EMAIL", "phone": "PHONE", "full_name": "PERSON_NAME", "name": "PERSON_NAME"}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^\+?[\d\-\s]{8,15}$")

# Dataset-level lineage (raw -> bronze -> silver -> gold -> warehouse). Column lineage for key metrics.
LINEAGE = [
    ("landing/batch/customers.csv", "bronze/customers"), ("landing/batch/orders.csv", "bronze/orders"),
    ("landing/stream/*.json", "silver_payments"),
    ("bronze/customers", "silver_customers"), ("bronze/orders", "silver_orders"), ("bronze/orders", "quarantine_orders"),
    ("silver_customers", "silver_orders"),
    ("silver_orders", "gold_daily_revenue"), ("silver_orders", "gold_customer_ltv"), ("silver_customers", "gold_customer_ltv"),
    ("silver_payments", "gold_payments_hourly"),
]
COLUMN_LINEAGE = {
    "gold_daily_revenue.revenue": ["silver_orders.amount", "silver_orders.status", "silver_orders.order_date"],
    "gold_customer_ltv.total_revenue": ["silver_orders.amount", "silver_orders.customer_id", "silver_customers.customer_id"],
    "gold_payments_hourly.success_amount": ["silver_payments.amount", "silver_payments.status"],
}


def _classify(con, table: str, col: str, typ: str) -> dict | None:
    for hint, kind in PII_NAME_HINTS.items():
        if col == hint or col.endswith("_" + hint):
            return {"pii": kind, "sensitivity": "HIGH", "evidence": "column name"}
    if typ.upper().startswith("VARCHAR"):
        vals = [r[0] for r in con.execute(f'SELECT "{col}" FROM {C.SCHEMA}.{table} WHERE "{col}" IS NOT NULL LIMIT 200').fetchall()]
        if vals:
            if sum(bool(EMAIL_RE.match(v)) for v in vals) / len(vals) > 0.8:
                return {"pii": "EMAIL", "sensitivity": "HIGH", "evidence": "value pattern"}
            if sum(bool(PHONE_RE.match(v)) for v in vals) / len(vals) > 0.8:
                return {"pii": "PHONE", "sensitivity": "HIGH", "evidence": "value pattern"}
    return None


def build(con=None) -> dict:
    own = con is None
    con = con or duckdb.connect(str(C.WAREHOUSE), read_only=True)
    glossary = yaml.safe_load((C.ROOT / "governance" / "glossary.yaml").read_text())["terms"]
    gl_by_col = {c: g["term"] for g in glossary for c in g["columns"]}
    tables = {}
    for (t,) in con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{C.SCHEMA}' ORDER BY 1").fetchall():
        cols = []
        for name, typ, *_ in con.execute(f"DESCRIBE {C.SCHEMA}.{t}").fetchall():
            cols.append({"name": name, "type": str(typ), "classification": _classify(con, t, name, str(typ)),
                         "glossary_term": gl_by_col.get(f"{t}.{name}")})
        n = con.execute(f"SELECT COUNT(*) FROM {C.SCHEMA}.{t}").fetchone()[0]
        up = [a for a, b in LINEAGE if b == t]
        tables[t] = {"rows": n, "columns": cols, "upstream": up, "pii_columns": [c["name"] for c in cols if c["classification"]]}
    cat = {"tables": tables, "lineage": LINEAGE, "column_lineage": COLUMN_LINEAGE, "glossary": glossary}
    if own:
        con.close()
    C.REPORTS.mkdir(parents=True, exist_ok=True)
    (C.REPORTS / "catalog.json").write_text(json.dumps(cat, indent=2))
    (C.REPORTS / "catalog.md").write_text(to_markdown(cat))
    return cat


def to_markdown(cat: dict) -> str:
    out = ["# Data catalog\n"]
    for t, m in cat["tables"].items():
        out.append(f"## Table {t}\nRows: {m['rows']}. Upstream: {', '.join(m['upstream']) or 'n/a'}. "
                   f"PII columns: {', '.join(m['pii_columns']) or 'none'}.\n")
        for c in m["columns"]:
            extra = f" PII={c['classification']['pii']}." if c["classification"] else ""
            gl = f" Glossary: {c['glossary_term']}." if c["glossary_term"] else ""
            out.append(f"- {t}.{c['name']} ({c['type']}).{extra}{gl}")
        out.append("")
    out.append("## Column lineage")
    for k, v in cat["column_lineage"].items():
        out.append(f"- {k} is derived from {', '.join(v)}.")
    out.append("\n## Business glossary")
    for g in cat["glossary"]:
        out.append(f"### {g['term']}\n{g['definition']}\n")
    return "\n".join(out)


if __name__ == "__main__":
    c = build()
    for t, m in c["tables"].items():
        print(t, m["rows"], "PII:", m["pii_columns"])
