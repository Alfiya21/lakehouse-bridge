"""LangGraph agent: plan -> act (tools via MCP or direct) -> answer -> verify.

- Planner is pluggable. RulePlanner is an offline, deterministic baseline (no API key needed).
  Plug an LLM in with `LLMPlanner(llm)` where llm(prompt:str)->str returns JSON (Gemini on Vertex AI, etc.).
- Verifier checks every number in the final answer appears in the tool observations (grounding check).
"""
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Callable, TypedDict

from langgraph.graph import StateGraph, END

KNOWN_TABLES = ["silver_customers", "silver_orders", "silver_payments", "quarantine_orders",
                "gold_daily_revenue", "gold_customer_ltv", "gold_payments_hourly"]
MUTATION = re.compile(r"\b(delete|drop|truncate|update|insert|alter|remove|erase|modify)\b", re.I)


class State(TypedDict, total=False):
    question: str
    plan: list
    refusal: str
    observations: list
    answer: str
    verified: bool
    attempts: int


# ----------------------------------------------------------------------------- planners
class RulePlanner:
    def plan(self, q: str) -> dict:
        ql = q.lower()
        if MUTATION.search(ql):
            return {"refuse": "This agent is read-only: it cannot modify or delete data."}
        t = next((t for t in KNOWN_TABLES if t in ql), None)
        n = re.search(r"top\s+(\d+)", ql)
        sql = lambda s: {"calls": [{"tool": "run_sql", "args": {"sql": s}}]}
        if "lineage" in ql or "come from" in ql or "derived from" in ql or "upstream" in ql:
            return {"calls": [{"tool": "lineage", "args": {"table": t or "gold_daily_revenue"}}]}
        if "quality" in ql or "failing" in ql or "data check" in ql:
            return {"calls": [{"tool": "quality_report", "args": {"table": t or ""}}]}
        if "what tables" in ql or "which tables" in ql or "available tables" in ql:
            return {"calls": [{"tool": "list_tables", "args": {}}]}
        if ("columns" in ql or "describe" in ql or "schema" in ql) and t:
            return {"calls": [{"tool": "describe_table", "args": {"table": t}}]}
        if any(w in ql for w in ("mean", "define", "definition", "glossary", "explain")):
            return {"calls": [{"tool": "search_docs", "args": {"query": q, "k": 3}}]}
        if "quarantin" in ql:
            return sql("SELECT reason, COUNT(*) AS orders FROM analytics.quarantine_orders GROUP BY reason ORDER BY orders DESC")
        if "email" in ql or "phone" in ql or "customer names" in ql:
            return sql("SELECT full_name, email, phone FROM analytics.silver_customers ORDER BY customer_id LIMIT 3")
        if "success rate" in ql or "payment" in ql:
            return sql("SELECT method, SUM(events) AS events, SUM(successes) AS successes, "
                       "ROUND(100.0*SUM(successes)/SUM(events), 2) AS success_pct FROM analytics.gold_payments_hourly GROUP BY method ORDER BY method")
        if "top" in ql and "customer" in ql:
            return sql(f"SELECT customer_id, region, total_revenue FROM analytics.gold_customer_ltv ORDER BY total_revenue DESC LIMIT {n.group(1) if n else 5}")
        if "revenue" in ql and "region" in ql:
            return sql("SELECT region, ROUND(SUM(revenue), 2) AS revenue FROM analytics.gold_daily_revenue GROUP BY region ORDER BY region")
        if "revenue" in ql and "month" in ql:
            return sql("SELECT strftime(order_date, '%Y-%m') AS month, ROUND(SUM(revenue), 2) AS revenue FROM analytics.gold_daily_revenue GROUP BY 1 ORDER BY 1 LIMIT 6")
        if "total revenue" in ql or ql.strip().startswith("revenue"):
            return sql("SELECT ROUND(SUM(revenue), 2) AS total_revenue FROM analytics.gold_daily_revenue")
        if "average order" in ql or "aov" in ql:
            return sql("SELECT ROUND(AVG(amount), 2) AS avg_order_value FROM analytics.silver_orders WHERE status <> 'CANCELLED'")
        if "how many customers" in ql or "number of customers" in ql:
            return sql("SELECT COUNT(*) AS customers FROM analytics.silver_customers")
        if "how many orders" in ql or "number of orders" in ql:
            return sql("SELECT COUNT(*) AS orders FROM analytics.silver_orders")
        return {"calls": [{"tool": "search_docs", "args": {"query": q, "k": 3}}]}


class LLMPlanner:
    """Wraps any `llm(prompt)->str`. The LLM must return JSON: {"calls":[{"tool":..,"args":{..}}]} or {"refuse":".."}.
    Falls back to RulePlanner if the output is not valid JSON."""
    PROMPT = ("You are a data agent with tools: list_tables, describe_table(table), run_sql(sql), quality_report(table), "
              "lineage(table), search_docs(query). Tables are in schema analytics. Read-only. Respond ONLY with JSON "
              '{{"calls":[{{"tool":"...","args":{{}}}}]}} or {{"refuse":"reason"}}.\nQuestion: {q}')

    def __init__(self, llm: Callable[[str], str]):
        self.llm, self.fallback = llm, RulePlanner()

    def plan(self, q: str) -> dict:
        try:
            out = json.loads(self.llm(self.PROMPT.format(q=q)))
            if "calls" in out or "refuse" in out:
                return out
        except Exception:
            pass
        return self.fallback.plan(q)


# ----------------------------------------------------------------------------- backends
class DirectBackend:
    def __init__(self, tools):
        self.tools = tools

    def call_many(self, calls: list) -> list:
        return [getattr(self.tools, c["tool"])(**c["args"]) for c in calls]


class McpBackend:
    """Runs the calls through a real MCP stdio session against lakehouse_bridge.agent.mcp_server."""

    def __init__(self, role: str):
        self.role = role

    def call_many(self, calls: list) -> list:
        return asyncio.run(self._run(calls))

    async def _run(self, calls):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        params = StdioServerParameters(command=sys.executable, args=["-m", "lakehouse_bridge.agent.mcp_server"],
                                       env={**os.environ, "LB_ROLE": self.role})
        src_dir = str(Path(__file__).resolve().parents[2])           # make the server importable in the child process
        params.env["PYTHONPATH"] = os.pathsep.join(filter(None, [src_dir, os.environ.get("PYTHONPATH", "")]))
        out = []
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                for c in calls:
                    res = await s.call_tool(c["tool"], c["args"])
                    out.append(json.loads(res.content[0].text))
        return out


# ----------------------------------------------------------------------------- answer + verify
def format_answer(q: str, obs: list) -> str:
    parts = []
    for o in obs:
        if "error" in o:
            parts.append(f"The request could not be completed: {o['error']}.")
        elif "rows" in o and "columns" in o:
            if not o["rows"]:
                parts.append("The query returned no rows.")
                continue
            lines = [", ".join(f"{c}={v}" for c, v in zip(o["columns"], r)) for r in o["rows"][:10]]
            parts.append(f"Result ({o['row_count']} rows): " + "; ".join(lines) + ".")
        elif "gate_passed" in o:
            fails = [f"{f['table']}: {f['description']} (failing_rows={f['failing_rows']})" for f in o["failing"]]
            parts.append(f"Quality gate passed={o['gate_passed']}; {o['passed']} of {o['rules']} rules passed"
                         + (f"; failing: {'; '.join(fails)}." if fails else "."))
        elif "upstream" in o and "table" in o and "columns" not in o:
            parts.append(f"{o['table']} is derived from: {', '.join(o['upstream']) or 'no upstream'}.")
        elif "columns" in o and "table" in o:
            parts.append(f"{o['table']} has {o['rows']} rows; columns: " + ", ".join(
                f"{c['name']}{' (PII, masked)' if c['masked_for_this_role'] else (' (PII)' if c['pii'] else '')}" for c in o["columns"]) + ".")
        elif "tables" in o:
            parts.append("Available tables: " + ", ".join(f"{t['table']} ({t['rows']} rows)" for t in o["tables"]) + ".")
        elif "matches" in o:
            parts.append(o["matches"][0]["text"].replace("\n", " ") if o["matches"] else "No relevant documentation found.")
    return " ".join(parts)


NUM = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?")


def grounded(answer: str, obs: list) -> bool:
    blob = json.dumps(obs, default=str)
    return all(n in blob for n in NUM.findall(answer))


# ----------------------------------------------------------------------------- graph
def build_graph(backend, planner=None, answerer: Callable | None = None):
    planner = planner or RulePlanner()
    answerer = answerer or format_answer

    def plan(s: State):
        p = planner.plan(s["question"])
        return {"plan": p.get("calls", []), "refusal": p.get("refuse", ""), "attempts": 0}

    def act(s: State):
        return {"observations": backend.call_many(s["plan"])}

    def answer(s: State):
        if s.get("refusal"):
            return {"answer": s["refusal"], "observations": []}
        return {"answer": answerer(s["question"], s["observations"]), "attempts": s.get("attempts", 0) + 1}

    def verify(s: State):
        ok = bool(s.get("refusal")) or grounded(s["answer"], s["observations"])
        if not ok and s["attempts"] >= 2:        # second failure: fall back to the deterministic formatter
            return {"verified": True, "answer": format_answer(s["question"], s["observations"])}
        return {"verified": ok}

    g = StateGraph(State)
    for name, fn in (("plan", plan), ("act", act), ("answer", answer), ("verify", verify)):
        g.add_node(name, fn)
    g.set_entry_point("plan")
    g.add_conditional_edges("plan", lambda s: "answer" if s.get("refusal") else "act", {"act": "act", "answer": "answer"})
    g.add_edge("act", "answer")
    g.add_edge("answer", "verify")
    g.add_conditional_edges("verify", lambda s: END if s["verified"] else "answer", {END: END, "answer": "answer"})
    return g.compile()


def ask(question: str, role: str = "admin", mcp: bool = False, planner=None) -> dict:
    from .tools import Tools
    backend = McpBackend(role) if mcp else DirectBackend(Tools(role))
    return build_graph(backend, planner).invoke({"question": question})


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "What is the total revenue by region?"
    role = os.environ.get("LB_ROLE", "admin")
    r = ask(q, role, mcp=os.environ.get("LB_MCP") == "1")
    print(r["answer"])
