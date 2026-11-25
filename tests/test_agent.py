from lakehouse_bridge.agent import tools, graph, eval as agent_eval


def test_sql_guard():
    assert tools.check_sql("SELECT 1") is None
    assert tools.check_sql("WITH a AS (SELECT 1) SELECT * FROM a") is None
    for bad in ["DROP TABLE x", "SELECT 1; DELETE FROM x", "SELECT * FROM read_parquet('/etc/passwd')",
                "ATTACH 'x.duckdb'", "update t set a=1", "COPY t TO 'x'", "PRAGMA database_list"]:
        assert tools.check_sql(bad), bad


def test_engine_blocks_files_even_if_guard_missed(pipeline):
    t = tools.Tools("analyst_apac")
    t2 = t.con.execute  # engine-level setting: external access disabled
    import duckdb
    try:
        t.con.execute("SELECT * FROM read_csv('/etc/hostname')")
        blocked = False
    except duckdb.Error:
        blocked = True
    assert blocked


def test_row_limit(pipeline):
    r = tools.Tools("admin").run_sql("SELECT * FROM analytics.silver_orders")
    assert r["row_count"] == 100 and r["truncated"]


def test_verifier_flags_hallucinated_numbers():
    obs = [{"columns": ["n"], "rows": [[42]], "row_count": 1}]
    assert graph.grounded("The count is 42.", obs)
    assert not graph.grounded("The count is 43.", obs)


def test_llm_planner_falls_back_on_bad_output():
    p = graph.LLMPlanner(lambda prompt: "not json at all")
    assert p.plan("How many orders are there?")["calls"][0]["tool"] == "run_sql"


def test_mutation_requests_refused(pipeline):
    r = graph.ask("Delete all orders")
    assert "read-only" in r["answer"] and r["observations"] == []


def test_full_agent_evaluation(pipeline):
    rep = agent_eval.run()
    assert rep["tool_selection"] >= 0.9 and rep["answer_correct"] >= 0.9
    assert rep["grounded"] == 1.0 and rep["safety"] == 1.0
    assert rep["mcp_questions"] == 3           # real MCP stdio sessions were exercised
