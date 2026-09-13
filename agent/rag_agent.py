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
    pip install databricks-langchain langchain databricks-sdk databricks-sql-connector
    set -a && source .env && set +a && python3 agent/rag_agent.py "What is a Kubernetes Deployment?"
"""
import sys
import time
import uuid
from datetime import datetime, timezone

from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
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


def _warehouse_http_path() -> str:
    return f"/sql/1.0/warehouses/{WAREHOUSE_ID}"


def _run_sql(statement: str, params: tuple = ()) -> list[dict]:
    with dbsql.connect(
        server_hostname=_w.config.host.replace("https://", ""),
        http_path=_warehouse_http_path(),
        access_token=_w.config.token,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


@tool
def search_kubernetes_docs(query: str) -> str:
    """Semantic search over the Kubernetes documentation chunks (Vector Search).
    Use this for conceptual/how-to questions about Kubernetes resources
    (Pods, Deployments, Services, Volumes, RBAC, etc.)."""
    vs = DatabricksVectorSearch(index_name=VS_INDEX_NAME, columns=["chunk_id", "parent_source", "section_path", "chunk_text"])
    docs = vs.similarity_search(query, k=5)
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
    aggregate lookups: rag_pipeline.default.rag_gold_doc_summary (per-doc
    chunk counts and sizes) and rag_pipeline.default.rag_gold_query_audit_log
    (history of past questions asked to this agent). Only SELECT statements
    are allowed. Always use fully-qualified table names."""
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


def log_turn(question: str, answer: str, tools_used: list[str], latency_ms: int, asked_by: str = "cli-user"):
    _run_sql(
        """
        INSERT INTO rag_pipeline.default.rag_gold_query_audit_log
        (query_id, asked_by, question, tools_used, retrieved_chunk_ids, answer_preview, latency_ms, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            asked_by,
            question,
            tools_used,
            _retrieved_chunk_ids[:20],
            answer[:500],
            latency_ms,
            datetime.now(timezone.utc),
        ),
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
                "Cite the source file for any claim drawn from search results.",
            ),
            ("human", "{input}"),
            MessagesPlaceholder("agent_scratchpad"),
        ]
    )
    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)


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
