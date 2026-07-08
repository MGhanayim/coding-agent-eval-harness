"""Unit tests for build_manifest (SPEC 2.3)."""
from datetime import datetime, timezone

from pipeline.artifacts import build_manifest, init_run_dir
from pipeline.config import resolve_config

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)


def test_manifest_paths_are_run_dir_relative(tmp_path):
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    manifest = build_manifest(paths)
    assert manifest["config"] == "config.json"
    assert manifest["metrics"] == "metrics.json"
    assert manifest["predictions"] == "run-agent/preds.json"
    assert manifest["trajectories"] == "run-agent/trajectories/"
    assert manifest["eval_logs"] == "run-eval/logs/"
    assert manifest["eval_reports"] == "run-eval/reports/"
    assert manifest["run_id"] == paths.run_id


def test_manifest_records_planned_remote_uri(tmp_path):
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    manifest = build_manifest(paths, remote_uri="s3://runs/some-run/")
    assert manifest["remote_artifact_uri"] == "s3://runs/some-run/"


def test_manifest_reads_mlflow_env_contract(tmp_path, monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "swe-bench-evals")
    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    manifest = build_manifest(paths)
    assert manifest["mlflow"] == {
        "tracking_uri": "http://mlflow:5000",
        "experiment": "swe-bench-evals",
    }


def test_manifest_experiment_matches_tracking_default(tmp_path, monkeypatch):
    # With the name unset, the manifest must record the SAME experiment
    # tracking.log_run would actually use — not "" (SPEC 2.3 pointer).
    monkeypatch.delenv("MLFLOW_EXPERIMENT_NAME", raising=False)
    from pipeline.tracking import experiment_name

    paths = init_run_dir(resolve_config(now=FIXED_NOW), root=tmp_path)
    manifest = build_manifest(paths)
    assert manifest["mlflow"]["experiment"] == experiment_name()
