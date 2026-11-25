"""Governed tools exposed to the agent. Used directly and through the MCP server.

Defence in depth for run_sql: (1) text guard (single SELECT/WITH statement, no DDL/DML, no file functions),
(2) read-only connection with external access disabled, (3) the connection only contains the data
this role is allowed to see (see governance.security), (4) hard row limit.
"""
import json
import re
import duckdb
from .. import config as C
from ..governance import security, catalog as catalog_mod
from . import rag

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|replace|attach|detach|copy|pragma|install|load|export|import|call|set|"
    r"read_csv\w*|read_parquet|read_json\w*|glob|read_text|parquet_scan|duckdb_\w+|pragma_\w+)\b", re.I)
MAX_ROWS = 100


def check_sql(sql: str) -> str | None:
    s = sql.strip().rstrip(";").strip()
    if not s:
        return "empty query"
    if ";" in s:
        return "only a single statement is allowed"
    if not re.match(r"^(select|with)\b", s, re.I):
        return "only SELECT queries are allowed"
    m = FORBIDDEN.search(s)
    if m:
        return f"keyword/function not allowed: {m.group(0)}"
    return None


class Tools:
    def __init__(self, role: str = "admin"):
        self.role = role
        self.policy = security.load_policy()
        self.con = duckdb.connect(str(security.role_db_path(role)), read_only=True,
                                  config={"enable_external_access": False})
        self.hidden = set(self.policy.get("restricted_tables", [])) if role != "admin" else set()
        self._docs = None

    # -- tools ---------------------------------------------------------------
    def list_tables(self) -> dict:
        rows = self.con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{C.SCHEMA}' ORDER BY 1").fetchall()
        out = [{"table": t, "rows": self.con.execute(f"SELECT COUNT(*) FROM {C.SCHEMA}.{t}").fetchone()[0]} for (t,) in rows]
        return {"role": self.role, "tables": out}

    def describe_table(self, table: str) -> dict:
        if table in self.hidden:
            return {"error": f"no access to table {table}"}
        cat = json.loads((C.REPORTS / "catalog.json").read_text())["tables"]
        if table not in cat:
            return {"error": f"unknown table {table}"}
        masked = self.policy["roles"][self.role]["mask_pii"]
        cols = [{"name": c["name"], "type": c["type"], "pii": (c["classification"] or {}).get("pii"),
                 "masked_for_this_role": bool(masked and c["classification"]), "glossary": c["glossary_term"]}
                for c in cat[table]["columns"]]
        return {"table": table, "rows": cat[table]["rows"], "columns": cols, "upstream": cat[table]["upstream"]}

    def run_sql(self, sql: str) -> dict:
        err = check_sql(sql)
        if err:
            return {"error": err}
        try:
            cur = self.con.execute(f"SELECT * FROM ({sql.strip().rstrip(';')}) LIMIT {MAX_ROWS + 1}")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        except Exception as e:  # surface engine errors to the agent so it can repair the query
            return {"error": str(e).splitlines()[0][:300]}
        return {"columns": cols, "rows": [list(r) for r in rows[:MAX_ROWS]], "row_count": min(len(rows), MAX_ROWS),
                "truncated": len(rows) > MAX_ROWS}

    def quality_report(self, table: str | None = None) -> dict:
        rep = json.loads((C.REPORTS / "quality_report.json").read_text())
        res = [r for r in rep["results"] if (table is None or r["table"] == table) and r["table"] not in self.hidden]
        return {"gate_passed": rep["gate_passed"], "rules": len(res), "failing": [r for r in res if not r["passed"]],
                "passed": sum(r["passed"] for r in res)}

    def lineage(self, table: str) -> dict:
        if table in self.hidden:
            return {"error": f"no access to table {table}"}
        edges = catalog_mod.LINEAGE
        seen, stack = [], [table]
        while stack:
            cur = stack.pop()
            for a, b in edges:
                if b == cur and a not in seen:
                    seen.append(a); stack.append(a)
        cols = {k: v for k, v in catalog_mod.COLUMN_LINEAGE.items() if k.startswith(table + ".")}
        return {"table": table, "upstream": seen, "column_lineage": cols}

    def search_docs(self, query: str, k: int = 3) -> dict:
        if self._docs is None:
            self._docs = rag.DocIndex.from_reports(hide_tables=self.hidden)
        return {"query": query, "matches": self._docs.search(query, k)}
