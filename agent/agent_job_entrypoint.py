# Databricks notebook source
# Databricks Job entrypoint for the RAG agent -- runs agent/rag_agent.py's
# `run()` on Databricks compute instead of a local laptop.
#
# Loads rag_agent.py's source directly via the Workspace export API and
# exec()s it into this notebook's globals, rather than `%run` (whose path
# resolver was consistently stripping the leading "/" off both
# "/Workspace/Users/..." and "/Users/..." forms, quoted or not -- a parser
# quirk in this workspace/runtime that a plain import can't work around
# either, since uploaded .py files become Notebook objects rather than
# files Python's import system can see). This approach only depends on the
# Workspace API, which has been reliable throughout this deployment.
#
# Trigger with a question via Workflows "Run now with different parameters",
# or via the Jobs API:
#     w.jobs.run_now(job_id=..., notebook_params={"question": "..."})

# COMMAND ----------

import base64

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

_w = WorkspaceClient()
_export = _w.workspace.export(
    "/Workspace/Users/lakshanilsah@gmail.com/rag_pipeline/agent/rag_agent.py",
    format=ExportFormat.SOURCE,
)
_source = base64.b64decode(_export.content).decode("utf-8")
# Exec into an isolated namespace (not globals()) so rag_agent.py's
# `if __name__ == "__main__":` block does NOT fire -- this notebook's own
# __name__ is already "__main__", and exec()ing into globals() directly
# would trigger rag_agent's CLI block (running the *default* question)
# before this notebook gets to ask its own widget-supplied question.
_rag_agent_ns = {"__name__": "rag_agent"}
exec(compile(_source, "rag_agent.py", "exec"), _rag_agent_ns)
run = _rag_agent_ns["run"]

# COMMAND ----------

dbutils.widgets.text("question", "What is a Kubernetes Deployment?")
question = dbutils.widgets.get("question")

answer = run(question)
print(answer)
