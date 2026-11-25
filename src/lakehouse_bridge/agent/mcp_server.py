"""MCP server exposing the governed tools over stdio. The role comes from LB_ROLE."""
import os
from mcp.server.fastmcp import FastMCP
from .tools import Tools

mcp = FastMCP("lakehouse-bridge")
_tools = None


def tools() -> Tools:
    global _tools
    if _tools is None:
        _tools = Tools(os.environ.get("LB_ROLE", "admin"))
    return _tools


@mcp.tool()
def list_tables() -> dict:
    """List the tables this role may query, with row counts."""
    return tools().list_tables()


@mcp.tool()
def describe_table(table: str) -> dict:
    """Describe a table: columns, types, PII classification, masking and upstream lineage."""
    return tools().describe_table(table)


@mcp.tool()
def run_sql(sql: str) -> dict:
    """Run one read-only SELECT query (DuckDB/BigQuery-compatible SQL). Tables live in schema analytics."""
    return tools().run_sql(sql)


@mcp.tool()
def quality_report(table: str = "") -> dict:
    """Latest data-quality results, optionally for one table."""
    return tools().quality_report(table or None)


@mcp.tool()
def lineage(table: str) -> dict:
    """Upstream dataset and column lineage of a table."""
    return tools().lineage(table)


@mcp.tool()
def search_docs(query: str, k: int = 3) -> dict:
    """Retrieve catalog, glossary and runbook passages relevant to the query (RAG)."""
    return tools().search_docs(query, k)


if __name__ == "__main__":
    mcp.run()
