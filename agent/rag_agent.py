"""
Hybrid RAG agent: combines semantic document search (Vector Search index over
rag_silver_chunks) with structural SQL lookups (Gold tables) behind a single
LangChain tool-calling agent.

Tools exposed to the LLM:
  - search_kubernetes_docs : semantic search over the Delta Sync Vector Search
      index (rag_chunks_index) -- "what does X mean / how do I configure Y".
  - query_gold_metrics     : read-only SQL Data Tool against the Gold layer
      (rag_gold_doc_summary, rag_gold_query_audit_log) -- "how many chunks
      came from services.md", "how many questions were asked last week".

Every turn is logged to rag_pipeline.default.rag_gold_query_audit_log so the
Data Tool itself can be queried for usage analytics (a hybrid query in its
own right).

Requires DATABRICKS_HOST / DATABRICKS_TOKEN in the environment:
    pip install databricks-langchain langchain databricks-sdk
    set -a && source .env && set +a && python3 agent/rag_agent.py "What is a Kubernetes Deployment?"
"""
import queue
import sys
import threading
import time
import uuid
from typing import Optional

TOOL_TIMEOUT_SECONDS = 20

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from databricks_langchain import ChatDatabricks, DatabricksVectorSearch
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool

CATALOG = "rag_pipeline"
SCHEMA = "default"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.rag_chunks_index"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
WAREHOUSE_ID = "3a31553ea9932c57"  # Serverless Starter Warehouse

_w = WorkspaceClient()
_retrieved_chunk_ids: list[str] = []  # per-turn scratch, reset in run()


def _run_with_timeout(fn, timeout: float = TOOL_TIMEOUT_SECONDS):
    """Run fn() in a daemon thread with a hard timeout.

    concurrent.futures.ThreadPoolExecutor was tried first, but its
    future.result(timeout=...) only stops *waiting* -- if fn() is genuinely
    stuck (not just slow), the interpreter still blocks at process exit
    joining that worker thread via ThreadPoolExecutor's own atexit hook, so
    a "timed out" tool call could still leave the whole job hanging. A
    daemon thread does not block process exit, so an abandoned/stuck call
    can never do that.
    """
    q: queue.Queue = queue.Queue(maxsize=1)

    def wrapper():
        try:
            q.put(("ok", fn()))
        except Exception as exc:  # noqa: BLE001
            q.put(("error", exc))

    threading.Thread(target=wrapper, daemon=True).start()
    try:
        status, value = q.get(timeout=timeout)
    except queue.Empty:
        raise TimeoutError(f"timed out after {timeout}s")
    if status == "error":
        raise value
    return value


def _run_sql(statement: str, params: Optional[dict] = None) -> list[dict]:
    """Run SQL via the Statement Execution API (not databricks-sql-connector's
    dbsql.connect(), which was found -- via a bisection diagnostic that timed
    each stage independently -- to hang indefinitely when called from inside
    Databricks job/notebook compute, even though WorkspaceClient init, the
    LLM call, and everything else completed in under 2s each). The Statement
    Execution API is what this whole project has used reliably everywhere
    else (the SQL MCP tool, Job sql_task entries), so reusing it here avoids
    a second, less reliable code path for the exact same thing.
    """
    param_items = [StatementParameterListItem(name=k, value=str(v)) for k, v in (params or {}).items()] or None
    resp = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        parameters=param_items,
        wait_timeout="30s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        resp = _w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    if not resp.manifest or not resp.manifest.total_row_count:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return [dict(zip(cols, row)) for row in rows]


def _similarity_search(query: str):
    vs = DatabricksVectorSearch(index_name=VS_INDEX_NAME, columns=["chunk_id", "parent_source", "section_path", "chunk_text"])
    return vs.similarity_search(query, k=5)


@tool
def search_kubernetes_docs(query: str) -> str:
    """Semantic search over the Kubernetes documentation chunks (Vector Search).
    Use this for conceptual/how-to questions about Kubernetes resources
    (Pods, Deployments, Services, Volumes, RBAC, etc.)."""
    # The vector-search client has been observed to hang rather than fail
    # fast when the endpoint/index doesn't exist or is unreachable (e.g.
    # torn down to save cost) -- a bad failure mode for a tool call inside
    # an agent loop. Enforce a hard timeout so the tool always returns
    # promptly, even in that case, instead of stalling the whole run.
    try:
        docs = _run_with_timeout(lambda: _similarity_search(query))
    except TimeoutError:
        return (
            "Vector search is unavailable (timed out after "
            f"{TOOL_TIMEOUT_SECONDS}s) -- the endpoint/index may not exist "
            "right now. Answer from query_gold_metrics or general knowledge instead, "
            "and say semantic search was unavailable."
        )
    except Exception as exc:
        return f"Vector search failed: {exc}"

    for d in docs:
        cid = d.metadata.get("chunk_id")
        if cid:
            _retrieved_chunk_ids.append(cid)
    return "\n\n---\n\n".join(
        f"[source: {d.metadata.get('parent_source')} | section: {d.metadata.get('section_path')}]\n{d.page_content}"
        for d in docs
    )


@tool
def query_gold_metrics(sql_query: str) -> str:
    """Run a read-only SQL query against the Gold layer for structural /
    aggregate lookups (NOT for semantic/topic search -- use
    search_kubernetes_docs for that). Only SELECT statements are allowed.
    Always use fully-qualified table names and exactly these columns:

    rag_pipeline.default.rag_gold_doc_summary
      parent_source STRING (e.g. 'deployments.md'), chunk_count BIGINT,
      total_chars BIGINT, avg_chunk_chars DOUBLE, first_silver_loaded_at
      TIMESTAMP, last_silver_loaded_at TIMESTAMP, file_size_bytes BIGINT,
      bronze_ingested_at TIMESTAMP.
      This table has no topic/content column -- it cannot answer "which doc
      has the most chunks about <topic>"; it can only answer "how many
      chunks/chars does <parent_source> have in total".

    rag_pipeline.default.rag_gold_query_audit_log
      query_id STRING, asked_by STRING, question STRING, tools_used
      ARRAY<STRING>, retrieved_chunk_ids ARRAY<STRING>, answer_preview
      STRING, latency_ms BIGINT, created_at TIMESTAMP.
    """
    normalized = sql_query.strip().rstrip(";")
    if not normalized.lower().startswith("select"):
        return "Error: only SELECT statements are permitted for this tool."
    try:
        rows = _run_sql(normalized)
    except Exception as exc:  # surfaced back to the LLM, not raised
        return f"Query failed: {exc}"
    if not rows:
        return "Query returned no rows."
    return "\n".join(str(r) for r in rows[:50])


def _sql_array_literal(values: list[str]) -> str:
    """Build an inline `array('a', 'b', ...)` SQL literal, quote-escaped.

    The Statement Execution API's named parameters are for scalar values;
    ARRAY<STRING> columns need a literal `array(...)` expression instead.
    Values here are always internally generated (tool names from a fixed
    set, or hex chunk_ids), never raw user input, so a simple quote-escape
    is sufficient.
    """
    if not values:
        return "CAST(array() AS ARRAY<STRING>)"
    escaped = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return f"array({escaped})"


def log_turn(question: str, answer: str, tools_used: list[str], latency_ms: int, asked_by: str = "cli-user"):
    tools_sql = _sql_array_literal(tools_used)
    chunks_sql = _sql_array_literal(_retrieved_chunk_ids[:20])
    _run_sql(
        f"""
        INSERT INTO rag_pipeline.default.rag_gold_query_audit_log
        (query_id, asked_by, question, tools_used, retrieved_chunk_ids, answer_preview, latency_ms, created_at)
        VALUES (:query_id, :asked_by, :question, {tools_sql}, {chunks_sql}, :answer_preview, :latency_ms, current_timestamp())
        """,
        {
            "query_id": str(uuid.uuid4()),
            "asked_by": asked_by,
            "question": question,
            "answer_preview": answer[:500],
            "latency_ms": latency_ms,
        },
    )


def build_agent() -> AgentExecutor:
    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)
    tools = [search_kubernetes_docs, query_gold_metrics]
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are a Kubernetes documentation assistant with two tools: "
                "semantic doc search and a SQL data tool over Gold summary/audit "
                "tables. Use search_kubernetes_docs for conceptual questions, "
                "query_gold_metrics for questions about corpus statistics or "
                "past usage, and combine both when a question needs both "
                "('which doc has the most chunks about Y, and what does it say'). "
                "Cite the source file for any claim drawn from search results. "
                "If a question needs a second tool, call it through the normal "
                "tool-calling mechanism -- never write out a function call as "
                "text in your answer. If you choose not to call a tool, just "
                "answer with what you already have instead of describing the "
                "tool call you would make.",
            ),
            ("human", "{input}"),
            MessagesPlaceholder("agent_scratchpad"),
        ]
    )
    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        max_iterations=6,
        return_intermediate_steps=True,
    )


def run(question: str) -> str:
    _retrieved_chunk_ids.clear()
    executor = build_agent()
    start = time.time()
    result = executor.invoke({"input": question})
    latency_ms = int((time.time() - start) * 1000)

    steps = result.get("intermediate_steps", [])
    tools_used = sorted({s[0].tool for s in steps}) if steps else []
    answer = result["output"]

    log_turn(question, answer, tools_used, latency_ms)
    return answer


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "What is a Kubernetes Deployment and how many doc chunks discuss it?"
    print(run(q))
