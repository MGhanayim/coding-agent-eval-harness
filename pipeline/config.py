"""Layer 0: run configuration. STDLIB ONLY — the Airflow DAG imports this module."""
from __future__ import annotations

import importlib.metadata
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

# Explicit run ids reach filesystem paths, glob patterns, MLflow filter
# strings, and S3 keys — restrict them to a safe slug so no sink needs
# escaping. Generated ids always match.
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

# The experiment knobs. Single source of truth for defaults (SPEC C1):
# the CLI's argparse flags and the Airflow trigger form are both built from
# this dict. Values here are decisions a researcher may vary between runs —
# anything the machine can generate, derive, or observe does NOT belong here.
PARAM_DEFAULTS: dict = {
    "split": "test",
    "subset": "verified",
    "model": "nebius/moonshotai/Kimi-K2.6",
    "task_slice": "0:3",
    "workers": 4,
    "cost_limit": 0.0,
}

# subset (the knob) -> HuggingFace dataset name the eval harness needs.
# Derived, not chosen: two knobs for one fact would let them drift apart.
SUBSET_DATASETS: dict[str, str] = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}

# Recorded into every config.json so a run dir alone answers "which agent/
# harness versions produced this?" (SPEC 2.2).
TRACKED_PACKAGES: tuple[str, ...] = ("mini-swe-agent", "swebench")


@dataclass(frozen=True)
class RunConfig:
    """Complete, immutable description of one evaluation run.

    Written once to runs/<run_id>/config.json by prepare-run; read (never
    modified) by every later pipeline step. Mirrors PLAN.md §7.
    """

    run_id: str
    created_at: str
    split: str
    subset: str
    workers: int
    model: str
    task_slice: str
    cost_limit: float
    dataset_name: str
    package_versions: dict[str, str]

    def to_json(self) -> str:
        """Serialize to the config.json wire format (indented, key-stable)."""
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> RunConfig:
        """Inverse of to_json; raises TypeError on missing/unknown fields."""
        return cls(**json.loads(text))


def runs_root() -> Path:
    """Root directory for run artifacts (env RUNS_ROOT, default ./runs)."""
    return Path(os.environ.get("RUNS_ROOT") or "./runs")


def mlflow_tracking_uri() -> str | None:
    """MLFLOW_TRACKING_URI, or None when tracking is not configured.
    Single accessor shared by tracking.py and artifacts.py so the manifest
    can never disagree with where the run was actually logged."""
    return os.environ.get("MLFLOW_TRACKING_URI") or None


def mlflow_experiment_name() -> str:
    """MLFLOW_EXPERIMENT_NAME with the project default (shared accessor)."""
    return os.environ.get("MLFLOW_EXPERIMENT_NAME") or "swe-bench-evals"


def load_env_file(path: Path, override: bool = False) -> None:
    """Minimal stdlib .env loader (KEY=VALUE lines, # comments, optional
    quotes). Exists so Layer 4 — which may import only this module — can
    load project env vars without python-dotenv being installed in the
    orchestrator environment. Missing file is a no-op."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        if override or not os.environ.get(key):
            os.environ[key] = value


def generate_run_id(now: datetime, subset: str, task_slice: str) -> str:
    """Build `<timestamp>__<subset>__<slice>` (PLAN §7), e.g.
    20260702T142530__verified__0-3. Deterministic given its inputs; the
    colon is replaced so the id is filesystem- and S3-key-safe."""
    stamp = now.strftime("%Y%m%dT%H%M%S")
    return f"{stamp}__{subset}__{task_slice.replace(':', '-')}"


def collect_package_versions(
    packages: tuple[str, ...] = TRACKED_PACKAGES,
) -> dict[str, str]:
    """Record installed versions of the packages that define run behavior."""
    versions: dict[str, str] = {}
    for pkg in packages:
        try:
            versions[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            versions[pkg] = "unknown"
    return versions


def resolve_config(
    overrides: dict | None = None,
    *,
    run_id: str | None = None,
    now: datetime | None = None,
) -> RunConfig:
    """Merge user overrides onto PARAM_DEFAULTS and freeze a complete RunConfig.

    Only PARAM_DEFAULTS keys may be overridden (unknown keys raise ValueError —
    a typo'd param must fail loudly, not silently fall back to a default).
    None values are treated as "not provided". `run_id` and `now` are
    injectable for reruns and tests; by default they are generated.
    """
    provided = {k: v for k, v in (overrides or {}).items() if v is not None}
    unknown = set(provided) - set(PARAM_DEFAULTS)
    if unknown:
        raise ValueError(f"unknown parameters: {sorted(unknown)}")
    if run_id is not None and not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            f"invalid run_id {run_id!r}: use only letters, digits, '.', '_', '-' "
            "(it becomes a directory name, an S3 prefix, and an MLflow tag)"
        )
    params = {**PARAM_DEFAULTS, **provided}
    if params["subset"] not in SUBSET_DATASETS:
        raise ValueError(
            f"unknown subset {params['subset']!r}; expected one of {sorted(SUBSET_DATASETS)}"
        )
    ts = now or datetime.now(timezone.utc)
    return RunConfig(
        run_id=run_id or generate_run_id(ts, params["subset"], params["task_slice"]),
        created_at=ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        split=str(params["split"]),
        subset=str(params["subset"]),
        workers=int(params["workers"]),
        model=str(params["model"]),
        task_slice=str(params["task_slice"]),
        cost_limit=float(params["cost_limit"]),
        dataset_name=SUBSET_DATASETS[params["subset"]],
        package_versions=collect_package_versions(),
    )
