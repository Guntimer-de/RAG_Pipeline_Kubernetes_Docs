"""
Provisions the Databricks Vector Search endpoint and Delta Sync Index for the
RAG silver layer. Safe to re-run (idempotent: checks for existing endpoint/index).

Requires DATABRICKS_HOST / DATABRICKS_TOKEN in the environment (see .env,
which is git-ignored -- never commit real credentials).

    pip install databricks-vectorsearch databricks-sdk
    set -a && source .env && set +a && python3 agent/create_vector_search_index.py
"""
import time

from databricks.vector_search.client import VectorSearchClient

ENDPOINT_NAME = "rag_vs_endpoint"
SOURCE_TABLE = "rag_pipeline.default.rag_silver_chunks"
INDEX_NAME = "rag_pipeline.default.rag_chunks_index"
EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"
PRIMARY_KEY = "chunk_id"
EMBEDDING_SOURCE_COLUMN = "chunk_text"


def ensure_endpoint(client: VectorSearchClient, name: str):
    existing = [e["name"] for e in client.list_endpoints().get("endpoints", [])]
    if name in existing:
        print(f"[SKIP] endpoint '{name}' already exists")
        return
    print(f"[CREATE] vector search endpoint '{name}' (type=STANDARD)")
    client.create_endpoint(name=name, endpoint_type="STANDARD")

    for _ in range(60):
        status = client.get_endpoint(name)["endpoint_status"]["state"]
        print(f"  endpoint state: {status}")
        if status == "ONLINE":
            return
        time.sleep(10)
    raise TimeoutError(f"Endpoint '{name}' did not come online in time")


def ensure_index(client: VectorSearchClient):
    try:
        client.get_index(index_name=INDEX_NAME)
        print(f"[SKIP] index '{INDEX_NAME}' already exists")
        return
    except Exception:
        pass

    print(f"[CREATE] delta sync index '{INDEX_NAME}' on '{SOURCE_TABLE}' (auto-sync, TRIGGERED)")
    client.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        source_table_name=SOURCE_TABLE,
        index_name=INDEX_NAME,
        pipeline_type="TRIGGERED",  # syncs automatically whenever the source table changes
        primary_key=PRIMARY_KEY,
        embedding_source_column=EMBEDDING_SOURCE_COLUMN,
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
    )


def main():
    client = VectorSearchClient()
    ensure_endpoint(client, ENDPOINT_NAME)
    ensure_index(client)
    print("\nDone. Index will auto-sync on every write to rag_silver_chunks (TRIGGERED pipeline).")
    print("Trigger a manual sync any time with: client.get_index(INDEX_NAME).sync()")


if __name__ == "__main__":
    main()
