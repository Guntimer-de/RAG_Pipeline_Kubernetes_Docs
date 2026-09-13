"""
Deploys this project's code into the Databricks workspace itself, so it's
visible/runnable from the console (Workspace file browser, Workflows), not
just runnable from a local checkout:

  1. Uploads the repo's Python/SQL files into
     /Workspace/Users/<you>/rag_pipeline/ (mirrors the local tree).
  2. Registers pipelines/silver_chunking_dlt.py as a real DLT pipeline
     (target: rag_pipeline.default), if one with this name doesn't exist yet.
  3. Creates a Job ("rag_pipeline_gold_refresh") with SQL tasks that refresh
     the Gold tables (sql/03_gold.sql) -- schedulable/runnable from Workflows.
     (Bronze/Silver ingestion is left as the DLT pipeline + the one-time
     local download step; see README.md for why.)

Idempotent: re-running skips anything that already exists by name.

Requires DATABRICKS_HOST / DATABRICKS_TOKEN in the environment:
    set -a && source .env && set +a && python3 agent/deploy_to_databricks.py
"""
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service.jobs import Task, NotebookTask, SqlTask, SqlTaskFile

REPO_ROOT = Path(__file__).resolve().parent.parent
CATALOG = "rag_pipeline"
SCHEMA = "default"
WAREHOUSE_ID = "3a31553ea9932c57"

FILES_TO_UPLOAD = [
    "Dataloading.sql",
    "sql/02_silver_chunks.sql",
    "sql/03_gold.sql",
    "pipelines/silver_chunking_dlt.py",
    "agent/rag_agent.py",
    "agent/create_vector_search_index.py",
    "scripts/01_download_data.py",
    "scripts/02_chunk_documents.py",
]


def upload_files(w: WorkspaceClient, base_path: str):
    for rel_path in FILES_TO_UPLOAD:
        local_path = REPO_ROOT / rel_path
        remote_path = f"{base_path}/{rel_path}"
        remote_dir = remote_path.rsplit("/", 1)[0]
        w.workspace.mkdirs(remote_dir)
        content = local_path.read_bytes()
        fmt = ImportFormat.AUTO if rel_path.endswith(".sql") else ImportFormat.SOURCE
        w.workspace.upload(remote_path, content, format=fmt, overwrite=True)
        print(f"[UPLOAD] {rel_path} -> {remote_path}")


def ensure_dlt_pipeline(w: WorkspaceClient, base_path: str):
    name = "rag_silver_chunking"
    existing = [p for p in w.pipelines.list_pipelines() if p.name == name]
    if existing:
        print(f"[SKIP] DLT pipeline '{name}' already exists ({existing[0].pipeline_id})")
        return

    pipeline = w.pipelines.create(
        name=name,
        catalog=CATALOG,
        target=SCHEMA,
        continuous=False,
        serverless=True,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=f"{base_path}/pipelines/silver_chunking_dlt.py"))],
    )
    print(f"[CREATE] DLT pipeline '{name}' ({pipeline.pipeline_id})")


def ensure_gold_refresh_job(w: WorkspaceClient, base_path: str):
    name = "rag_pipeline_gold_refresh"
    existing = [j for j in w.jobs.list(name=name) if j.settings and j.settings.name == name]
    if existing:
        print(f"[SKIP] Job '{name}' already exists ({existing[0].job_id})")
        return

    job = w.jobs.create(
        name=name,
        tasks=[
            Task(
                task_key="refresh_gold_tables",
                sql_task=SqlTask(
                    warehouse_id=WAREHOUSE_ID,
                    file=SqlTaskFile(path=f"{base_path}/sql/03_gold.sql"),
                ),
            )
        ],
    )
    print(f"[CREATE] Job '{name}' ({job.job_id})")


def main():
    w = WorkspaceClient()
    me = w.current_user.me().user_name
    base_path = f"/Workspace/Users/{me}/rag_pipeline"
    print(f"Deploying to {base_path} as {me}")

    upload_files(w, base_path)
    ensure_dlt_pipeline(w, base_path)
    ensure_gold_refresh_job(w, base_path)

    print("\nDone. Check Workspace > Users > {} > rag_pipeline, and Workflows.".format(me))


if __name__ == "__main__":
    main()
