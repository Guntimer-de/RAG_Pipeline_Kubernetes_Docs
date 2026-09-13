-- ============================================================================
-- RETIRED -- kept for reference only. DO NOT RUN AGAINST A LIVE WORKSPACE.
--
-- rag_pipeline.default.rag_silver_chunks is now owned exclusively by the
-- rag_silver_chunking DLT pipeline (pipelines/silver_chunking_dlt.py).
-- Running this file's CREATE OR REPLACE TABLE would strip the table back to
-- a plain managed table, which breaks the DLT pipeline's next run (a DLT
-- pipeline can only materialize tables it created itself -- this is exactly
-- the table-ownership collision documented in README.md's "Notes" section).
--
-- This file documents the schema and the load pattern originally used to
-- populate the table from ./data/processed/chunks.jsonl via
-- scripts/02_chunk_documents.py, before the DLT pipeline became the single
-- source of truth.
--
-- delta.enableChangeDataFeed = true is required so the Vector Search
-- Delta Sync Index (see agent/create_vector_search_index.py) can
-- incrementally sync on every write to this table.
-- ============================================================================

CREATE OR REPLACE TABLE rag_pipeline.default.rag_silver_chunks (
  chunk_id STRING NOT NULL,
  parent_source STRING,
  source_file STRING,
  chunk_index INT,
  section_path STRING,
  chunk_text STRING,
  char_count INT,
  created_at TIMESTAMP,
  silver_loaded_at TIMESTAMP,
  CONSTRAINT rag_silver_chunks_pk PRIMARY KEY (chunk_id)
)
COMMENT 'Silver layer: chunked + metadata-enriched document text, ready for embedding/indexing'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Stage the locally-computed chunks.jsonl into the volume first:
--   PUT '<local>/data/processed/chunks.jsonl'
--   INTO '/Volumes/rag_pipeline/default/rag_raw_volume/_staging/chunks.jsonl' OVERWRITE;

INSERT INTO rag_pipeline.default.rag_silver_chunks
SELECT
  chunk_id,
  parent_source,
  source_file,
  CAST(chunk_index AS INT),
  section_path,
  chunk_text,
  CAST(char_count AS INT),
  CAST(created_at AS TIMESTAMP),
  current_timestamp() AS silver_loaded_at
FROM read_files(
  '/Volumes/rag_pipeline/default/rag_raw_volume/_staging/chunks.jsonl',
  format => 'json'
);

-- Sanity check
SELECT parent_source, count(*) AS chunk_count, avg(char_count) AS avg_chars
FROM rag_pipeline.default.rag_silver_chunks
GROUP BY parent_source
ORDER BY parent_source;

-- Example: compute an embedding ad hoc via the Foundation Model API (no cluster needed)
-- SELECT chunk_id, ai_query('databricks-gte-large-en', chunk_text) AS embedding
-- FROM rag_pipeline.default.rag_silver_chunks
-- LIMIT 5;
