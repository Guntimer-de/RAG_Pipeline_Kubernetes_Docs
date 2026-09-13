"""
Downloads a curated set of Kubernetes concept docs (Markdown, CC BY 4.0) from the
kubernetes/website GitHub repo into ./data/raw/, plus a manifest.json with
per-file metadata (source url, local path, byte size, sha256, download timestamp).

Source: https://github.com/kubernetes/website (content/en/docs/concepts/...)
License: CC BY 4.0 (https://github.com/kubernetes/website/blob/main/LICENSE)
"""
import hashlib
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAW_BASE = "https://raw.githubusercontent.com/kubernetes/website/main/content/en/docs"

# (relative source path in the k8s docs repo, local filename)
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

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download(url: str, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "rag-pipeline-demo/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    failures = []

    for rel_path, filename in PAGES:
        url = f"{RAW_BASE}/{rel_path}"
        dest = DATA_DIR / filename
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
                "local_path": str(dest),
                "source_url": url,
                "file_size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"[OK] {filename} ({len(content)} bytes)")

    manifest_path = DATA_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nDownloaded {len(manifest)}/{len(PAGES)} files. Manifest: {manifest_path}")

    if failures:
        print(f"{len(failures)} file(s) failed, see warnings above.", file=sys.stderr)
        if not manifest:
            sys.exit(1)


if __name__ == "__main__":
    main()
