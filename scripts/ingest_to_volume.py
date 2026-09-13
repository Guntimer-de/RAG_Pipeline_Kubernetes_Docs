"""
Databricks-native ingestion task: downloads the Kubernetes concept docs
directly into the UC Volume, with no local machine involved.

Runs as a Python task on Databricks job compute. Volumes are mounted as a
normal POSIX path on job/cluster nodes, so this writes straight to
/Volumes/rag_pipeline/default/rag_raw_volume/ with plain file I/O -- no
presigned-URL upload dance needed (that was only required when running from
a local laptop outside the workspace; see scripts/01_download_data.py for
that local-bootstrap variant).

Intended as a task in the rag_pipeline_full_refresh Job (see README.md).
"""
import hashlib
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAW_BASE = "https://raw.githubusercontent.com/kubernetes/website/main/content/en/docs"
VOLUME_DIR = Path("/Volumes/rag_pipeline/default/rag_raw_volume")

PAGES = [
    ("concepts/overview/_index.md", "what-is-kubernetes.md"),
    ("concepts/overview/components.md", "cluster-components.md"),
    ("concepts/workloads/pods/_index.md", "pods.md"),
    ("concepts/workloads/controllers/deployment.md", "deployments.md"),
    ("concepts/workloads/controllers/statefulset.md", "statefulsets.md"),
    ("concepts/services-networking/service.md", "services.md"),
    ("concepts/services-networking/ingress.md", "ingress.md"),
    ("concepts/storage/volumes.md", "volumes.md"),
    ("concepts/storage/persistent-volumes.md", "persistent-volumes.md"),
    ("concepts/configuration/configmap.md", "configmaps.md"),
    ("concepts/configuration/secret.md", "secrets.md"),
    ("concepts/scheduling-eviction/kube-scheduler.md", "kube-scheduler.md"),
    ("concepts/architecture/nodes.md", "nodes.md"),
    ("concepts/security/rbac-good-practices.md", "rbac-good-practices.md"),
]


def download(url: str, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "rag-pipeline-demo/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main() -> None:
    VOLUME_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    failures = []

    for rel_path, filename in PAGES:
        url = f"{RAW_BASE}/{rel_path}"
        dest = VOLUME_DIR / filename
        try:
            content = download(url)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            print(f"[WARN] failed to download {url}: {exc}", file=sys.stderr)
            failures.append({"url": url, "error": str(exc)})
            continue

        dest.write_bytes(content)
        manifest.append(
            {
                "file_name": filename,
                "volume_path": str(dest),
                "source_url": url,
                "file_size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"[OK] {filename} ({len(content)} bytes) -> {dest}")

    manifest_dir = VOLUME_DIR / "_manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"\nLanded {len(manifest)}/{len(PAGES)} files directly in the UC volume.")

    if failures:
        print(f"{len(failures)} file(s) failed, see warnings above.", file=sys.stderr)
        if not manifest:
            sys.exit(1)


if __name__ == "__main__":
    main()
