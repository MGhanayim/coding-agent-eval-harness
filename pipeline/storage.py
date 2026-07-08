"""Layer 2: durable artifacts on S3-compatible object storage.

Portability trick (PLAN §11): boto3 reads AWS_ENDPOINT_URL /
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the environment, so the same
code talks to MinIO locally and Nebius Object Storage in production — only
.env changes. S3 has no real directories: "uploading a folder" means one
put_object per file under a shared key prefix.
"""
from __future__ import annotations

import os
from pathlib import Path

import boto3

from pipeline.artifacts import RunPaths


def bucket_name() -> str:
    return os.environ.get("RUNS_BUCKET", "runs")


def storage_enabled() -> bool:
    """Storage is opt-in by env: no endpoint configured → skip upload."""
    return bool(os.environ.get("AWS_ENDPOINT_URL"))


def planned_uri(run_id: str) -> str:
    """The S3 destination for a run — computable *before* upload, so the
    manifest inside the uploaded copy already points at itself (PLAN §6)."""
    return f"s3://{bucket_name()}/{run_id}/"


def make_s3_client():
    """Client from env. boto3 picks up AWS_ENDPOINT_URL and credentials
    itself; being explicit here keeps the contract greppable."""
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))


def upload_run_dir(paths: RunPaths, client=None) -> str:
    """Upload the whole run dir under s3://<bucket>/<run_id>/ (overwriting —
    retries are idempotent). Returns the destination URI."""
    client = client or make_s3_client()
    bucket = bucket_name()
    files = sorted(p for p in paths.root.rglob("*") if p.is_file())
    for path in files:
        key = f"{paths.run_id}/{path.relative_to(paths.root)}"
        client.upload_file(str(path), bucket, key)
    return planned_uri(paths.run_id)


def download_run_dir(run_id: str, destination: Path, client=None) -> Path:
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
