# RAG Pipeline: Kubernetes Docs on Databricks

An end-to-end, medallion-architecture RAG pipeline on Databricks Unity Catalog
and Vector Search, built over a corpus of official Kubernetes concept docs
(Markdown, CC BY 4.0, downloaded from `kubernetes/website`). Runs natively on
Databricks (Jobs + DLT), not orchestrated from a local machine.

## Architecture

```
Internet (kubernetes/website raw .md)
        │  scripts/ingest_to_volume.py   -- Job task "ingest_raw_files"
        ▼                                   (runs on Databricks compute,
Volumes/rag_pipeline/default/rag_raw_volume   writes straight to the volume)
        │
        │  Dataloading.sql   -- Job task "refresh_bronze" (SQL warehouse)
        ▼
rag_pipeline.default.rag_bronze_files            [BRONZE: file metadata]
        │
        │  pipelines/silver_chunking_dlt.py -- Job task "run_silver_dlt_pipeline"
        │  (triggers the rag_silver_chunking DLT pipeline; markdown
        │   header-boundary split + recursive char splitting, natively in Spark)
        ▼
rag_pipeline.default.rag_silver_chunks           [SILVER: 546 chunks, CDF enabled]
        │
        ├──▶ ai_query('databricks-gte-large-en', chunk_text)   [ad hoc embeddings via SQL]
        │
        └──▶ agent/create_vector_search_index.py  (one-time/on-demand setup, not
                 │                                  part of the scheduled refresh --
                 ▼                                  see "Vector Search" below)
        rag_vs_endpoint (STANDARD)  ──Delta Sync Index (TRIGGERED, auto-syncs on write)──▶
        rag_pipeline.default.rag_chunks_index

        sql/03_gold.sql -- Job task "refresh_gold" (SQL warehouse)
        ▼
rag_pipeline.default.rag_gold_doc_summary        [GOLD: per-doc chunk stats]
rag_pipeline.default.rag_gold_query_audit_log    [GOLD: agent usage/audit log]
        │
        ▼
agent/rag_agent.py  (LangChain tool-calling agent, databricks-meta-llama-3-3-70b-instruct)
    ├── search_kubernetes_docs   → Vector Search tool (semantic)
    └── query_gold_metrics       → SQL Data Tool over Gold tables (structural)
    -- runs as the rag_pipeline_agent_query Job (on-demand, parameterized),
       not as a script on a laptop
```

The four data tasks above are chained as one Databricks Job,
**`rag_pipeline_full_refresh`**, with each task depending on the previous one
(`ingest_raw_files` → `refresh_bronze` → `run_silver_dlt_pipeline` →
`refresh_gold`). Trigger it from Workflows, or:
```python
w.jobs.run_now(job_id=<rag_pipeline_full_refresh job id>)
```

## Unity Catalog objects

| Object | Type | Purpose |
|---|---|---|
| `rag_pipeline.default.rag_raw_volume` | Volume | Landing zone for raw downloaded files |
| `rag_pipeline.default.rag_bronze_files` | Delta table | File-level metadata (path, size, timestamps) |
| `rag_pipeline.default.rag_silver_chunks` | Delta table | Chunked text + metadata, CDF enabled. Owned by the `rag_silver_chunking` DLT pipeline -- a DLT pipeline can only materialize tables it creates itself, so this is the single source of truth (no parallel local-load path) |
| `rag_pipeline.default.rag_gold_doc_summary` | Delta table | Per-document chunk statistics |
| `rag_pipeline.default.rag_gold_query_audit_log` | Delta table | Agent query/tool-use audit trail |
| `rag_vs_endpoint` | Vector Search endpoint | STANDARD tier. **Torn down by default to avoid idle billing** -- recreate with `agent/create_vector_search_index.py` before asking the agent anything that needs semantic search |
| `rag_pipeline.default.rag_chunks_index` | Delta Sync Index | Embeds via `databricks-gte-large-en`, auto-syncs on Silver writes |

## Databricks Workflows

| Job | Tasks | Trigger |
|---|---|---|
| `rag_pipeline_full_refresh` | `ingest_raw_files` → `refresh_bronze` → `run_silver_dlt_pipeline` → `refresh_gold` | Manual / on a schedule you add |
| `rag_pipeline_agent_query` | `ask_agent` (parameterized by `question`) | Manual, with a `question` parameter -- see below |

All code also lives in the Workspace file browser under
`/Workspace/Users/<you>/rag_pipeline/`, mirroring this repo.

## Repo layout

- `scripts/ingest_to_volume.py` — Databricks-native download task: fetches
  the corpus and writes straight into the UC Volume from job compute (no
  local machine, no presigned-URL upload). Used by the
  `rag_pipeline_full_refresh` Job.
- `scripts/01_download_data.py` / `scripts/02_chunk_documents.py` — the
  original local-laptop bootstrap scripts (download to `./data/raw/`, chunk
  to `./data/processed/chunks.jsonl`) kept as a reference implementation and
  for offline dev/testing; not part of the deployed pipeline.
- `Dataloading.sql`, `sql/03_gold.sql` — DDL/DML run as SQL tasks in the
  full-refresh Job. `sql/02_silver_chunks.sql` documents the retired
  SQL-load path for Silver (superseded by the DLT pipeline, kept for
  reference).
- `pipelines/silver_chunking_dlt.py` — the production Delta Live Tables
  pipeline (Bronze read → markdown-aware chunking → Silver), registered as
  `rag_silver_chunking`.
- `agent/create_vector_search_index.py` — provisions the Vector Search
  endpoint + Delta Sync Index via the `databricks-vectorsearch` SDK. Not
  part of the scheduled Job (it's persistent infra, not a per-run artifact,
  and re-running it needlessly would mean paying for a second standing
  endpoint) -- run on demand when you want to ask the agent anything.
- `agent/rag_agent.py` — the hybrid LangChain agent (semantic + structural
  tools), with every turn logged to the Gold audit table. Deployed as the
  `rag_pipeline_agent_query` Job; also runnable locally for dev (see below).
- `agent/agent_job_entrypoint.py` — thin Databricks notebook wrapper around
  `rag_agent.py::run()`: reads the `question` job parameter via
  `dbutils.widgets` and loads `rag_agent.py`'s code via the Workspace export
  API + `exec()` (not a plain import -- see "Notes" below for why).

## Running the agent

**On Databricks (production path):**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
run = w.jobs.run_now(
    job_id=<rag_pipeline_agent_query job id>,
    notebook_params={"question": "What is a Kubernetes Deployment?"},
)
```
Or from Workflows: open `rag_pipeline_agent_query` → **Run now with different parameters** → set `question`.

**Locally (dev/debugging only):**
```bash
pip install -r requirements.txt
cp .env.example .env   # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
set -a && source .env && set +a
python3 agent/rag_agent.py "What is a Kubernetes Deployment?"
```

Either way, if you get "Vector search is unavailable" in the answer, it's
because `rag_vs_endpoint` has been torn down for cost control -- run
`python3 agent/create_vector_search_index.py` first.

## Chunking strategy

Markdown-aware, two-phase (implemented identically in `pipelines/silver_chunking_dlt.py`
and, as a reference, in `scripts/02_chunk_documents.py`):
1. **Semantic boundary split** on header lines (`#`…`######`), so a chunk
   never spans two unrelated sections. Each chunk records its full header
   path (e.g. `Deployments > Updating a Deployment > Rollover`) as
   `section_path` metadata.
2. **Recursive character split** within each section on a separator cascade
   (`\n\n` → `\n` → `. ` → ` `), target size 1200 chars, 150 char overlap —
   mirrors `RecursiveCharacterTextSplitter` semantics without the dependency.

## Notes / known constraints

- Data source is CC BY 4.0 (Kubernetes docs), not public domain — permissive
  and freely redistributable, attribution preserved via `source_url` in the
  volume's `_manifest/manifest.json`.
- `rag_vs_endpoint` is a billable STANDARD Vector Search endpoint — tear it
  down (`VectorSearchClient().delete_endpoint("rag_vs_endpoint")`) when not
  in use.
- `.env` holds the workspace host/token used by local dev scripts; it is
  git-ignored and must never be committed. On Databricks itself, everything
  authenticates via the notebook/job's native identity -- no token needed.
- **Pin exact dependency versions in any Databricks `JobEnvironment` spec.**
  An unpinned `dependencies` list (e.g. just `"langchain"`) was observed to
  make pip's resolver hang for 10+ minutes inside this workspace's
  serverless compute; pinning (`langchain==0.3.30`, etc., matching
  `requirements.txt`) brought the same environment build down to ~100s.
- **`agent_job_entrypoint.py` loads `rag_agent.py` via the Workspace export
  API + `exec()`, not a plain `import`.** Uploaded `.py` files become
  Databricks Notebook objects, not files Python's import system can see
  (even via `/Workspace/...` paths), and `%run`'s path resolver was found to
  strip the leading `/` off any path tried here, quoted or not. Exporting
  the source and `exec()`-ing it into an isolated namespace (not `globals()`)
  sidesteps both issues, and also avoids accidentally firing
  `rag_agent.py`'s own `if __name__ == "__main__":` block.
- **The agent's SQL access uses the Statement Execution API
  (`w.statement_execution`), not `databricks-sql-connector`'s `dbsql.connect()`.**
  The latter was found -- via a staged bisection test -- to hang
  indefinitely when called from inside Databricks job/notebook compute,
  even though every other stage (`WorkspaceClient()` init, the LLM call
  itself) completed in under 2 seconds. Also worth knowing generally: a
  `concurrent.futures.ThreadPoolExecutor`-based timeout only stops
  *waiting* on a stuck call -- its atexit hook still joins the worker
  thread before the process can exit, so a "timed out" tool can still hang
  the whole job. `agent/rag_agent.py`'s `_run_with_timeout` uses a plain
  daemon `threading.Thread` instead, which does not block process exit.
