"""Unit tests for pipeline.storage (URI planning + upload key layout).

Uses a recording stub instead of a live client — the S3 *wire* is exercised
against real MinIO in the Block G smoke test.
"""
from datetime import datetime, timezone

from pipeline.artifacts import init_run_dir
from pipeline.config import resolve_config
from pipeline.storage import planned_uri, storage_enabled, upload_run_dir

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)


class RecordingClient:
    def __init__(self):
        self.uploads: list[tuple[str, str, str]] = []

    def upload_file(self, filename, bucket, key):
        self.uploads.append((filename, bucket, key))


def test_planned_uri_is_computable_before_upload(monkeypatch):
    monkeypatch.setenv("RUNS_BUCKET", "runs")
    assert planned_uri("some-run") == "s3://runs/some-run/"


def test_storage_disabled_without_endpoint(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    assert not storage_enabled()
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    assert storage_enabled()


def test_upload_mirrors_run_dir_under_run_id_prefix(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNS_BUCKET", "runs")
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    paths.metrics_path.write_text("{}")
    client = RecordingClient()

    uri = upload_run_dir(paths, client=client)

    assert uri == f"s3://runs/{paths.run_id}/"
    keys = {key for _, bucket, key in client.uploads if bucket == "runs"}
    assert f"{paths.run_id}/config.json" in keys
    assert f"{paths.run_id}/metrics.json" in keys
