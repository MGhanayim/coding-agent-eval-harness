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
    def __init__(self, existing: dict[str, int] | None = None):
        self.uploads: list[tuple[str, str, str]] = []
        self._existing = existing or {}

    def upload_file(self, filename, bucket, key):
        self.uploads.append((filename, bucket, key))

    def get_paginator(self, operation):
        existing = self._existing

        class Paginator:
            def paginate(self, **kwargs):
                yield {
                    "Contents": [
                        {"Key": key, "Size": size} for key, size in existing.items()
                    ]
                }

        return Paginator()


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


def test_retry_upload_skips_unchanged_files_but_resends_roots(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNS_BUCKET", "runs")
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    paths.metrics_path.write_text("{}")
    trajectory = paths.trajectories_dir / "big.traj.json"
    trajectory.write_text("x" * 100)

    already_uploaded = {
        f"{paths.run_id}/run-agent/trajectories/big.traj.json": 100,
        f"{paths.run_id}/config.json": paths.config_path.stat().st_size,
    }
    client = RecordingClient(existing=already_uploaded)
    upload_run_dir(paths, client=client)

    keys = {key for _, _, key in client.uploads}
    assert f"{paths.run_id}/run-agent/trajectories/big.traj.json" not in keys
    assert f"{paths.run_id}/config.json" in keys  # mutable roots always re-sent


def test_upload_skips_symlinks(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNS_BUCKET", "runs")
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    (paths.root / "dangling").symlink_to("/nonexistent/target")
    client = RecordingClient()
    upload_run_dir(paths, client=client)
    assert not any(key.endswith("/dangling") for _, _, key in client.uploads)
