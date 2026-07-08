"""Layer 2: MLflow tracking — the queryable cross-run comparison layer.

MLflow stores numbers and *references*, never artifact copies (PLAN §9):
the run dir stays on disk/S3; here we log params + metrics + pointers.

Idempotency contract (PLAN §6): summarize_and_log retries must not create
duplicate MLflow runs, so log_run() searches for an existing run tagged
`run_id=<id>` and resumes it instead of starting a new one.
"""
from __future__ import annotations

import dataclasses
import os

import mlflow

from pipeline.config import RunConfig


def tracking_uri() -> str | None:
    """MLFLOW_TRACKING_URI, or None when tracking is not configured."""
    return os.environ.get("MLFLOW_TRACKING_URI") or None


def experiment_name() -> str:
    return os.environ.get("MLFLOW_EXPERIMENT_NAME", "swe-bench-evals")


def _find_existing_run(run_id: str) -> str | None:
    """Return the MLflow run id already tagged with our pipeline run_id."""
    hits = mlflow.search_runs(
        filter_string=f"tags.run_id = '{run_id}'", output_format="list"
    )
    return hits[0].info.run_id if hits else None


def log_run(
    config: RunConfig,
    metrics: dict,
    artifact_uri: str = "",
    local_path: str = "",
) -> str:
    """Log one pipeline run to MLflow; returns the MLflow run id.

    Params = the full RunConfig (SPEC 3.1, package versions flattened);
    metrics = the counters + resolve_rate (SPEC 3.2); artifact locations
    ride as tags (SPEC 3.3) — tags stay mutable, so a retry that gains an
    S3 URI can update them, while immutable params stay identical.
    """
    mlflow.set_tracking_uri(tracking_uri())
    mlflow.set_experiment(experiment_name())

    params = dataclasses.asdict(config)
    for package, version in params.pop("package_versions").items():
        params[f"version.{package}"] = version

    with mlflow.start_run(
        run_id=_find_existing_run(config.run_id), run_name=config.run_id
    ) as active:
        mlflow.set_tag("run_id", config.run_id)
        mlflow.log_params(params)
        mlflow.log_metrics({key: float(value) for key, value in metrics.items()})
        if local_path:
            mlflow.set_tag("artifact_local_path", local_path)
        if artifact_uri:
            mlflow.set_tag("artifact_remote_uri", artifact_uri)
        return active.info.run_id
