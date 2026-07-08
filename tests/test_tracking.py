"""Unit tests for pipeline.tracking against a sqlite-backed MLflow store —
no server needed; the live server is exercised in the Block F smoke test."""
from datetime import datetime, timezone

from pipeline.config import resolve_config
from pipeline.tracking import log_run

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)
METRICS = {
    "total_instances": 500,
    "submitted_instances": 1,
    "completed_instances": 1,
    "resolved_instances": 1,
    "unresolved_instances": 0,
    "empty_patch_instances": 0,
    "error_instances": 0,
    "resolve_rate": 1.0,
}


def _configure_file_store(tmp_path, monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path}/mlflow.db")
    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "test-experiment")


def test_log_run_records_params_metrics_and_reference(tmp_path, monkeypatch):
    import mlflow

    _configure_file_store(tmp_path, monkeypatch)
    config = resolve_config(now=FIXED_NOW)

    mlflow_run_id = log_run(
        config, METRICS, artifact_uri="s3://runs/x/", local_path="runs/x"
    )

    run = mlflow.get_run(mlflow_run_id)
    assert run.data.params["model"] == config.model
    assert run.data.params["task_slice"] == config.task_slice
    assert run.data.metrics["resolve_rate"] == 1.0
    assert run.data.tags["run_id"] == config.run_id
    assert run.data.tags["artifact_remote_uri"] == "s3://runs/x/"


def test_log_run_is_idempotent_by_run_id_tag(tmp_path, monkeypatch):
    _configure_file_store(tmp_path, monkeypatch)
    config = resolve_config(now=FIXED_NOW)

    first = log_run(config, METRICS)
    second = log_run(config, METRICS, artifact_uri="s3://runs/late-uri/")

    assert first == second  # found-and-resumed, not duplicated
