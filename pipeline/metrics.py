"""Layer 2: turn the harness's batch summary into comparable numbers.

The harness summary (run-eval/reports/summary.json) is authoritative but
verbose (full instance-id lists). metrics.json distills it to the counters
worth comparing across runs (PLAN §7), plus the derived headline number:

    resolve_rate = resolved / submitted

resolved/submitted — not /total (total is the whole dataset, not this run)
and not /completed (an agent crash must hurt the score, not shrink the
denominator).
"""
from __future__ import annotations

import json

from pipeline.artifacts import RunPaths

COUNTER_KEYS: tuple[str, ...] = (
    "total_instances",
    "submitted_instances",
    "completed_instances",
    "resolved_instances",
    "unresolved_instances",
    "empty_patch_instances",
    "error_instances",
)


def summarize_counts(summary: dict) -> dict:
    """Distill a harness summary dict into metrics (pure, unit-testable).
    A missing counter raises KeyError — schema drift must fail loudly."""
    metrics = {key: summary[key] for key in COUNTER_KEYS}
    submitted = metrics["submitted_instances"]
    metrics["resolve_rate"] = (
        round(metrics["resolved_instances"] / submitted, 3) if submitted else 0.0
    )
    return metrics


def collect_metrics(paths: RunPaths) -> dict:
    """Parse this run's summary.json into the metrics dict."""
    summary_path = paths.eval_reports_dir / "summary.json"
    return summarize_counts(json.loads(summary_path.read_text()))


def write_metrics(paths: RunPaths, metrics: dict) -> None:
    """Persist metrics.json at the run root (SPEC 2.1)."""
    paths.metrics_path.write_text(json.dumps(metrics, indent=2))
