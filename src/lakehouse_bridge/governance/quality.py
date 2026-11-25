"""Rule-driven data quality engine. Every rule compiles to one SQL query that counts failing rows."""
import json
import yaml
import duckdb
from .. import config as C

RULES_FILE = C.ROOT / "governance" / "rules.yaml"


def _sql(r: dict, S: str) -> tuple[str, str]:
    t, c = f"{S}.{r['table']}", r.get("column")
    k = r["rule"]
    if k == "not_null":
        return f"SELECT COUNT(*) FROM {t} WHERE {c} IS NULL", f"{c} must not be null"
    if k == "unique":
        return f"SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) n FROM {t} GROUP BY {c} HAVING COUNT(*) > 1)", f"{c} must be unique"
    if k == "min":
        return f"SELECT COUNT(*) FROM {t} WHERE {c} < {r['min']}", f"{c} >= {r['min']}"
    if k == "accepted_values":
        vals = ", ".join(f"'{v}'" for v in r["values"])
        return f"SELECT COUNT(*) FROM {t} WHERE {c} IS NOT NULL AND {c} NOT IN ({vals})", f"{c} in {r['values']}"
    if k == "regex":
        return f"SELECT COUNT(*) FROM {t} WHERE {c} IS NOT NULL AND NOT regexp_matches({c}, '{r['pattern']}')", f"{c} matches pattern"
    if k == "completeness":
        return (f"SELECT CASE WHEN 1.0 * COUNT({c}) / NULLIF(COUNT(*), 0) >= {r['min_ratio']} THEN 0 ELSE 1 END FROM {t}",
                f"{c} completeness >= {r['min_ratio']}")
    if k == "relationship":
        rt = f"{S}.{r['ref_table']}"
        return (f"SELECT COUNT(*) FROM {t} a LEFT JOIN {rt} b ON a.{c} = b.{r['ref_column']} WHERE b.{r['ref_column']} IS NULL",
                f"{c} references {r['ref_table']}.{r['ref_column']}")
    if k == "max_ratio_of":
        rt = f"{S}.{r['ref_table']}"
        return (f"SELECT CASE WHEN 1.0 * (SELECT COUNT(*) FROM {t}) / NULLIF((SELECT COUNT(*) FROM {rt}), 0) <= {r['max_ratio']} THEN 0 ELSE 1 END",
                f"row count <= {r['max_ratio']:.0%} of {r['ref_table']}")
    raise ValueError(f"unknown rule {k}")


def run(con=None) -> dict:
    own = con is None
    con = con or duckdb.connect(str(C.WAREHOUSE), read_only=True)
    rules = yaml.safe_load(RULES_FILE.read_text())["rules"]
    results = []
    for r in rules:
        sql, desc = _sql(r, C.SCHEMA)
        failing = con.execute(sql).fetchone()[0]
        results.append({"table": r["table"], "rule": r["rule"], "column": r.get("column"), "description": desc,
                        "severity": r["severity"], "failing_rows": int(failing), "passed": failing == 0})
    errors = [x for x in results if not x["passed"] and x["severity"] == "error"]
    warns = [x for x in results if not x["passed"] and x["severity"] == "warn"]
    report = {"total_rules": len(results), "passed": sum(x["passed"] for x in results),
              "error_failures": len(errors), "warnings": len(warns), "gate_passed": not errors, "results": results}
    if own:
        con.close()
    C.REPORTS.mkdir(parents=True, exist_ok=True)
    (C.REPORTS / "quality_report.json").write_text(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    rep = run()
    for x in rep["results"]:
        flag = "PASS" if x["passed"] else ("FAIL" if x["severity"] == "error" else "WARN")
        print(f"{flag}  {x['table']:20s} {x['description']:48s} failing={x['failing_rows']}")
    print({k: v for k, v in rep.items() if k != "results"})
    raise SystemExit(0 if rep["gate_passed"] else 1)
