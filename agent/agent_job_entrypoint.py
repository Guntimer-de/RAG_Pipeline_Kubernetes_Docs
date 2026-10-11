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
