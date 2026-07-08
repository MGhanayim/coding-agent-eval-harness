"""Layer 2: durable artifacts on S3-compatible object storage.

Portability trick (PLAN §11): boto3 reads AWS_ENDPOINT_URL /
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the environment, so the same
code talks to MinIO locally and Nebius Object Storage in production — only
.env changes. S3 has no real directories: "uploading a folder" means one
put_object per file under a shared key prefix.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import boto3

from pipeline.artifacts import RunPaths


def bucket_name() -> str:
    """Bucket for run artifacts (env RUNS_BUCKET; empty string falls back
    to the default too — docker-mode env passthrough may deliver "")."""
    return os.environ.get("RUNS_BUCKET") or "runs"


def storage_enabled() -> bool:
    """Storage is opt-in by env: no endpoint configured → skip upload."""
    return bool(os.environ.get("AWS_ENDPOINT_URL"))


def planned_uri(run_id: str) -> str:
    """The S3 destination for a run — computable *before* upload, so the
    manifest inside the uploaded copy already points at itself (PLAN §6)."""
    return f"s3://{bucket_name()}/{run_id}/"


def make_s3_client() -> Any:
    """Client from env. boto3 picks up AWS_ENDPOINT_URL and credentials
    itself; being explicit here keeps the contract greppable."""
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))


def _existing_sizes(client: Any, bucket: str, run_id: str) -> dict[str, int]:
    """Key → size for everything already uploaded under this run's prefix."""
    sizes: dict[str, int] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{run_id}/"):
        for obj in page.get("Contents", []):
            sizes[obj["Key"]] = obj["Size"]
    return sizes


def upload_run_dir(paths: RunPaths, client: Any = None, max_workers: int = 8) -> str:
    """Upload the run dir under s3://<bucket>/<run_id>/. Retry-friendly and
    cheap: files whose size already matches the remote copy are skipped
    (except the small mutable roots — metrics/manifest/config — which are
    always re-sent), and uploads run concurrently. Symlinks are skipped:
    they dangle outside this machine. Returns the destination URI."""
    client = client or make_s3_client()
    bucket = bucket_name()
    always_send = {paths.metrics_path, paths.manifest_path, paths.config_path}
    existing = _existing_sizes(client, bucket, paths.run_id)

    def send(path: Path) -> None:
        key = f"{paths.run_id}/{path.relative_to(paths.root)}"
        if (
            path not in always_send
            and existing.get(key) == path.stat().st_size
        ):
            return
        client.upload_file(str(path), bucket, key)

    files = sorted(
        p for p in paths.root.rglob("*") if p.is_file() and not p.is_symlink()
    )
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(send, files))  # drain to surface the first exception
    return planned_uri(paths.run_id)


def download_run_dir(run_id: str, destination: Path, client: Any = None) -> Path:
    """Fetch s3://<bucket>/<run_id>/ into destination/<run_id>/ — the
    SPEC 2.4 reconstruction test, as code."""
    client = client or make_s3_client()
    bucket = bucket_name()
    target_root = destination / run_id
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{run_id}/"):
        for obj in page.get("Contents", []):
            relative = obj["Key"][len(run_id) + 1 :]
            target = target_root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, obj["Key"], str(target))
    return target_root
