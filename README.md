# RAG Pipeline: Kubernetes Docs on Databricks

An end-to-end, medallion-architecture RAG pipeline on Databricks Unity Catalog
and Vector Search, built over a corpus of official Kubernetes concept docs
(Markdown, CC BY 4.0, downloaded from `kubernetes/website`).

## Architecture

```
Internet (kubernetes/website raw .md)
        │  scripts/01_download_data.py
        ▼
./data/raw/*.md  ──PUT (presigned URL)──▶  Volumes/rag_pipeline/default/rag_raw_volume
        │
        │  Dataloading.sql  (read_files binaryFile, no cluster needed)
        ▼
rag_pipeline.default.rag_bronze_files            [BRONZE: file metadata]
        │
        │  scripts/02_chunk_documents.py (local)     ─┐
        │  pipelines/silver_chunking_dlt.py (DLT)      ├─ markdown header-boundary
        │                                              │  + recursive char splitting
        ▼                                             ─┘
rag_pipeline.default.rag_silver_chunks           [SILVER: 546 chunks, CDF enabled]
        │
        ├──▶ ai_query('databricks-gte-large-en', chunk_text)   [ad hoc embeddings via SQL]
        │
        └──▶ agent/create_vector_search_index.py
                 │
                 ▼
        rag_vs_endpoint (STANDARD)  ──Delta Sync Index (TRIGGERED, auto-syncs on write)──▶
        rag_pipeline.default.rag_chunks_index

rag_pipeline.default.rag_gold_doc_summary        [GOLD: per-doc chunk stats]
rag_pipeline.default.rag_gold_query_audit_log    [GOLD: agent usage/audit log]
        │
        ▼
agent/rag_agent.py  (LangChain tool-calling agent, databricks-meta-llama-3-3-70b-instruct)
    ├── search_kubernetes_docs   → Vector Search tool (semantic)
    └── query_gold_metrics       → SQL Data Tool over Gold tables (structural)
```

## Unity Catalog objects

| Object | Type | Purpose |
|---|---|---|
| `rag_pipeline.default.rag_raw_volume` | Volume | Landing zone for raw downloaded files |
| `rag_pipeline.default.rag_bronze_files` | Delta table | File-level metadata (path, size, timestamps) |
| `rag_pipeline.default.rag_silver_chunks` | Delta table | Chunked text + metadata, CDF enabled |
| `rag_pipeline.default.rag_gold_doc_summary` | Delta table | Per-document chunk statistics |
| `rag_pipeline.default.rag_gold_query_audit_log` | Delta table | Agent query/tool-use audit trail |
| `rag_vs_endpoint` | Vector Search endpoint | STANDARD tier |
| `rag_pipeline.default.rag_chunks_index` | Delta Sync Index | Embeds via `databricks-gte-large-en`, auto-syncs on Silver writes |

## Repo layout

- `scripts/01_download_data.py` — downloads the raw corpus into `./data/raw/`.
- `scripts/02_chunk_documents.py` — local reference implementation of the
  chunking strategy (markdown header split + recursive character split with
  overlap); writes `./data/processed/chunks.jsonl`.
- `Dataloading.sql`, `sql/02_silver_chunks.sql`, `sql/03_gold.sql` — the
  actual DDL/DML run against the workspace SQL warehouse to build
  Bronze/Silver/Gold (pure SQL, no cluster required).
- `pipelines/silver_chunking_dlt.py` — production Delta Live Tables pipeline
  that performs the same chunking strategy natively in Spark/DLT, for
  deployment as a scheduled/streaming pipeline instead of the local script.
- `agent/create_vector_search_index.py` — provisions the Vector Search
  endpoint + Delta Sync Index via the `databricks-vectorsearch` SDK.
- `agent/rag_agent.py` — the hybrid LangChain agent (semantic + structural
  tools), with every turn logged to the Gold audit table.

## Running it end to end

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
set -a && source .env && set +a

python3 scripts/01_download_data.py
python3 scripts/02_chunk_documents.py
# run Dataloading.sql, sql/02_silver_chunks.sql, sql/03_gold.sql against your
# SQL warehouse (e.g. via the Databricks SQL editor, or any SQL client)

python3 agent/create_vector_search_index.py
python3 agent/rag_agent.py "What is a Kubernetes Deployment?"
```

To deploy the Spark-native chunking pipeline instead of the local script,
create a DLT pipeline pointing at `pipelines/silver_chunking_dlt.py` with
target catalog/schema `rag_pipeline` / `default`.

## Chunking strategy

Markdown-aware, two-phase:
1. **Semantic boundary split** on header lines (`#`…`######`), so a chunk
   never spans two unrelated sections. Each chunk records its full header
   path (e.g. `Deployments > Updating a Deployment > Rollover`) as
   `section_path` metadata.
2. **Recursive character split** within each section on a separator cascade
   (`\n\n` → `\n` → `. ` → ` `), target size 1200 chars, 150 char overlap —
   mirrors `RecursiveCharacterTextSplitter` semantics without the dependency
   in the local script; the DLT pipeline implements the identical algorithm
   natively in PySpark.

## Notes / known constraints

- Data source is CC BY 4.0 (Kubernetes docs), not public domain — permissive
  and freely redistributable, attribution preserved via `source_url` in
  `data/raw/manifest.json`.
- `rag_vs_endpoint` is a billable STANDARD Vector Search endpoint — tear it
  down (`VectorSearchClient().delete_endpoint("rag_vs_endpoint")`) when not
  in use.
- `.env` holds the workspace host/token used by the SDK-based scripts; it is
  git-ignored and must never be committed.
