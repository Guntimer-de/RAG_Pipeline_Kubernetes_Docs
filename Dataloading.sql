-- ============================================================================
-- Bronze layer: Unity Catalog volume + raw file metadata table
-- Catalog: rag_pipeline | Schema: default
-- Run order: this file, then sql/02_silver_chunks.sql, then sql/03_gold.sql
-- ============================================================================

CREATE CATALOG IF NOT EXISTS rag_pipeline;

CREATE SCHEMA IF NOT EXISTS rag_pipeline.default;

-- Managed UC Volume that holds the raw downloaded documents
-- (files are landed here from ./scripts/01_download_data.py via the
--  SQL `PUT ... INTO '/Volumes/...'` volume-upload API)
CREATE VOLUME IF NOT EXISTS rag_pipeline.default.rag_raw_volume;

-- Bronze table: tracks file-level metadata for everything landed in the volume.
-- Uses read_files(..., format => 'binaryFile') so this can be re-run any time
-- (e.g. as a scheduled job) to pick up newly landed files without needing a
-- Spark cluster -- it runs entirely on a SQL warehouse.
CREATE OR REPLACE TABLE rag_pipeline.default.rag_bronze_files
COMMENT 'Bronze layer: raw file metadata for documents landed in the UC volume rag_raw_volume'
AS
SELECT
  path AS file_path,
  regexp_extract(path, '([^/]+)$', 1) AS file_name,
  length AS file_size_bytes,
  modificationTime AS file_modified_at,
  current_timestamp() AS bronze_ingested_at
FROM read_files(
  '/Volumes/rag_pipeline/default/rag_raw_volume/',
  format => 'binaryFile'
);

-- Sanity check
SELECT file_name, file_size_bytes, file_modified_at
FROM rag_pipeline.default.rag_bronze_files
ORDER BY file_name;
