-- ============================================================================
-- Gold layer: structured summary metrics + query audit log
-- Catalog: rag_pipeline | Schema: default
--
-- These are the tables the agent's "Data Tool" (agent/rag_agent.py) queries
-- via plain SQL for structural lookups, alongside the Vector Search tool for
-- semantic lookups -- the hybrid retrieval pattern.
-- ============================================================================

-- Per-document rollup: chunk counts, sizes, freshness -- refresh by re-running.
CREATE OR REPLACE TABLE rag_pipeline.default.rag_gold_doc_summary
COMMENT 'Gold: per-source-document chunk statistics, derived from bronze + silver'
AS
SELECT
  s.parent_source,
  count(*) AS chunk_count,
  sum(s.char_count) AS total_chars,
  round(avg(s.char_count), 1) AS avg_chunk_chars,
  min(s.silver_loaded_at) AS first_silver_loaded_at,
  max(s.silver_loaded_at) AS last_silver_loaded_at,
  b.file_size_bytes,
  b.bronze_ingested_at
FROM rag_pipeline.default.rag_silver_chunks s
JOIN rag_pipeline.default.rag_bronze_files b
  ON s.source_file = b.file_path
GROUP BY s.parent_source, b.file_size_bytes, b.bronze_ingested_at;

-- Audit log: one row per agent turn, capturing which tool(s) it used and what
-- it retrieved. The agent appends here (see agent/rag_agent.py::log_turn);
-- this table also acts as the "Data Tool" target for meta-questions like
-- "how many questions were asked about Services last week?".
CREATE TABLE IF NOT EXISTS rag_pipeline.default.rag_gold_query_audit_log (
  query_id STRING NOT NULL,
  asked_by STRING,
  question STRING,
  tools_used ARRAY<STRING>,
  retrieved_chunk_ids ARRAY<STRING>,
  answer_preview STRING,
  latency_ms BIGINT,
  created_at TIMESTAMP,
  CONSTRAINT rag_audit_log_pk PRIMARY KEY (query_id)
)
COMMENT 'Gold: audit trail of hybrid agent queries -- which tools ran, what was retrieved';

-- Example hybrid lookups the "Data Tool" can run:
-- SELECT parent_source, chunk_count, avg_chunk_chars FROM rag_pipeline.default.rag_gold_doc_summary ORDER BY chunk_count DESC;
-- SELECT count(*) FROM rag_pipeline.default.rag_gold_query_audit_log WHERE question ILIKE '%service%' AND created_at > current_date() - INTERVAL 7 DAYS;
