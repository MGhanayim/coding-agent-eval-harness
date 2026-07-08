"""Layer 1: the runs/<run-id>/ artifact tree — the reproducibility contract.

SPEC 2.1 shape:
    runs/<run-id>/
    ├── config.json
    ├── run-agent/
    │   ├── preds.json
    │   └── trajectories/
    ├── run-eval/
    │   ├── logs/
    │   └── reports/
    ├── metrics.json
    └── manifest.json
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from pipeline.config import RunConfig, runs_root


@dataclass(frozen=True)
class RunPaths:
    """Every artifact path derived from a single root (PLAN §10).

    No other module may build a runs/ path by hand — one source of truth
    means one place to change the layout, and zero drift between writers
    and readers.
    """

    root: Path

    @classmethod
    def for_run(cls, run_id: str, root: Path | None = None) -> RunPaths:
        """Locate the run dir for `run_id` under `root` (default RUNS_ROOT)."""
        return cls(root=(root if root is not None else runs_root()) / run_id)

    @property
    def run_id(self) -> str:
        return self.root.name

    @property
    def config_path(self) -> Path:
        return self.root / "config.json"

    @property
    def agent_dir(self) -> Path:
        return self.root / "run-agent"

    @property
    def trajectories_dir(self) -> Path:
        return self.agent_dir / "trajectories"

    @property
    def preds_path(self) -> Path:
        return self.agent_dir / "preds.json"

    @property
    def eval_dir(self) -> Path:
        return self.root / "run-eval"

    @property
    def eval_logs_dir(self) -> Path:
        return self.eval_dir / "logs"

    @property
    def eval_reports_dir(self) -> Path:
        return self.eval_dir / "reports"

    @property
    def metrics_path(self) -> Path:
        return self.root / "metrics.json"

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.json"


def init_run_dir(config: RunConfig, root: Path | None = None) -> RunPaths:
    """Create the SPEC 2.1 tree and write config.json. Fails loudly
    (FileExistsError) if the run dir already exists — a duplicate run_id
    must never silently overwrite a previous run's artifacts."""
    paths = RunPaths.for_run(config.run_id, root)
    paths.root.mkdir(parents=True, exist_ok=False)
    paths.trajectories_dir.mkdir(parents=True)
    paths.eval_logs_dir.mkdir(parents=True)
    paths.eval_reports_dir.mkdir(parents=True)
    paths.config_path.write_text(config.to_json())
    return paths


def load_config(paths: RunPaths) -> RunConfig:
    """Read back the frozen config that governs this run."""
    return RunConfig.from_json(paths.config_path.read_text())


def build_manifest(paths: RunPaths, remote_uri: str = "") -> dict:
    """The "send this folder to a teammate" index (SPEC 2.3, PLAN §7).

    Entries are run-dir-relative and derived from RunPaths, so the manifest
    can never disagree with the actual layout. `remote_uri` is the *planned*
    S3 destination — written before upload so the uploaded copy already
    points at itself (PLAN §6); empty until Block G wires storage in.
    """

    def rel(path: Path, trailing_slash: bool = False) -> str:
        value = str(path.relative_to(paths.root))
        return value + "/" if trailing_slash else value

    return {
        "run_id": paths.run_id,
        "config": rel(paths.config_path),
        "metrics": rel(paths.metrics_path),
        "predictions": rel(paths.preds_path),
        "trajectories": rel(paths.trajectories_dir, trailing_slash=True),
        "eval_logs": rel(paths.eval_logs_dir, trailing_slash=True),
        "eval_reports": rel(paths.eval_reports_dir, trailing_slash=True),
        "remote_artifact_uri": remote_uri,
        "mlflow": {
            "tracking_uri": os.environ.get("MLFLOW_TRACKING_URI", ""),
            "experiment": os.environ.get("MLFLOW_EXPERIMENT_NAME", ""),
        },
    }


def write_manifest(paths: RunPaths, manifest: dict) -> None:
    """Persist manifest.json at the run root (SPEC 2.1)."""
    paths.manifest_path.write_text(json.dumps(manifest, indent=2))
