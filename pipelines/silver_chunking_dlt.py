"""
Delta Live Tables pipeline: Bronze (raw files) -> Silver (chunked text).

Deploy as a DLT pipeline with this file as the library source. Configure the
pipeline's target catalog/schema as `rag_pipeline` / `default` and point the
source volume at `/Volumes/rag_pipeline/default/rag_raw_volume`.

This mirrors the chunking strategy implemented locally in
scripts/02_chunk_documents.py (markdown header-boundary splitting, then
recursive character splitting with overlap) so results are consistent
whether the pipeline runs on a SQL warehouse (ad hoc, via Dataloading.sql /
sql/02_silver_chunks.sql) or on a Spark cluster via this DLT pipeline.

Requires: Databricks Runtime with `dlt` available (any DLT pipeline cluster).
"""
import hashlib
import re
from datetime import datetime, timezone

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_VOLUME_PATH = "/Volumes/rag_pipeline/default/rag_raw_volume"

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 150
SEPARATORS = ["\n\n", "\n", ". ", " "]

HEADER_RE = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)

CHUNK_SCHEMA = ArrayType(
    StructType(
        [
            StructField("chunk_id", StringType()),
            StructField("chunk_index", IntegerType()),
            StructField("section_path", StringType()),
            StructField("chunk_text", StringType()),
            StructField("char_count", IntegerType()),
        ]
    )
)


def _split_by_headers(text: str):
    matches = list(HEADER_RE.finditer(text))
    if not matches:
        return [("", text)]

    segments = []
    header_stack = []
    preamble = text[: matches[0].start()].strip()
    if preamble:
        segments.append(("", preamble))

    for i, m in enumerate(matches):
        level = len(m.group(1))
        title = m.group(2).strip()
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end]
        header_stack = [h for h in header_stack if h[0] < level]
        header_stack.append((level, title))
        segments.append((" > ".join(h[1] for h in header_stack), body))
    return segments


def _recursive_split(text: str, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP, seps=None):
    seps = seps if seps is not None else SEPARATORS
    text = text.strip()
    if len(text) <= size:
        return [text] if text else []
    if not seps:
        chunks, start = [], 0
        while start < len(text):
            end = min(start + size, len(text))
            chunks.append(text[start:end])
            start = end - overlap if end < len(text) else end
        return chunks

    sep, rest = seps[0], seps[1:]
    parts = text.split(sep)
    chunks, current = [], ""
    for part in parts:
        candidate = (current + sep + part) if current else part
        if len(candidate) <= size:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(part) > size:
                chunks.extend(_recursive_split(part, size, overlap, rest))
                current = ""
            else:
                current = part
    if current:
        chunks.append(current)

    if overlap and len(chunks) > 1:
        overlapped = [chunks[0]]
        for i in range(1, len(chunks)):
            overlapped.append((chunks[i - 1][-overlap:] + sep + chunks[i]).strip())
        return overlapped
    return chunks


def chunk_markdown(path: str, text: str):
    """Pandas/Python UDF body: markdown -> list of chunk structs."""
    records, idx = [], 0
    for section_path, section_text in _split_by_headers(text or ""):
        for piece in _recursive_split(section_text):
            piece = piece.strip()
            if not piece:
                continue
            chunk_id = hashlib.sha256(f"{path}:{idx}:{piece[:64]}".encode()).hexdigest()[:24]
            records.append((chunk_id, idx, section_path, piece, len(piece)))
            idx += 1
    return records


chunk_markdown_udf = F.udf(chunk_markdown, CHUNK_SCHEMA)


@dlt.table(
    name="rag_bronze_files_dlt",
    comment="Bronze: raw markdown files auto-loaded from the UC volume (streaming, schema-evolving).",
    table_properties={"quality": "bronze"},
)
def rag_bronze_files_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", f"{RAW_VOLUME_PATH}/_schema")
        .load(RAW_VOLUME_PATH)
        # input_file_name() isn't supported under Unity Catalog governed
        # streaming reads -- _metadata.file_path is the UC-compatible
        # equivalent.
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="rag_silver_chunks",
    comment="Silver: markdown-aware, recursively chunked document text, ready for embedding/indexing. Produced natively by this DLT pipeline -- the single source of truth for Gold and the agent.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("non_empty_chunk", "char_count > 0")
@dlt.expect("reasonable_chunk_size", "char_count <= 2000")
def rag_silver_chunks():
    bronze = (
        dlt.read_stream("rag_bronze_files_dlt")
        .groupBy("file_path")
        .agg(F.concat_ws("\n", F.collect_list("value")).alias("full_text"))
    )

    exploded = bronze.withColumn("chunks", chunk_markdown_udf("file_path", "full_text")).withColumn(
        "chunk", F.explode("chunks")
    )

    return exploded.select(
        F.col("chunk.chunk_id").alias("chunk_id"),
        F.regexp_extract("file_path", r"([^/]+)$", 1).alias("parent_source"),
        F.col("file_path").alias("source_file"),
        F.col("chunk.chunk_index").alias("chunk_index"),
        F.col("chunk.section_path").alias("section_path"),
        F.col("chunk.chunk_text").alias("chunk_text"),
        F.col("chunk.char_count").alias("char_count"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("silver_loaded_at"),
    )
