"""Layer 2: judge the agent's patches with the SWE-bench harness.

The harness writes relative to its cwd: `logs/run_evaluation/<run_id>/
<model-slug>/<instance>/...` plus a `<model-slug>.<run_id>.json` summary.
We run it with cwd=run-eval/ so everything lands inside the run dir, then
reshape to the SPEC 2.1 contract:

    run-eval/logs/<instance_id>/{report.json, patch.diff, test_output.txt, ...}
    run-eval/reports/{summary.json, <instance_id>.json}
"""
from __future__ import annotations

import shutil
import subprocess
import sys

from pipeline.artifacts import RunPaths
from pipeline.config import RunConfig


def build_eval_command(config: RunConfig, paths: RunPaths) -> list[str]:
    """Translate a RunConfig into the harness argv (pure)."""
    return [
        sys.executable, "-m", "swebench.harness.run_evaluation",
        "--dataset_name", config.dataset_name,
        "--split", config.split,
        "--predictions_path", str(paths.preds_path),
        "--max_workers", str(config.workers),
        "--run_id", config.run_id,
    ]


def relocate_eval_outputs(config: RunConfig, paths: RunPaths) -> None:
    """Reshape raw harness output into the run-dir contract.

    Flattens logs/run_evaluation/<run_id>/<model-slug>/<instance>/ down to
    logs/<instance>/, moves the summary to reports/summary.json, and copies
    each per-instance report.json to reports/<instance>.json so verdicts are
    readable without digging through logs (SPEC 2.2 / PLAN §8 W2).
    """
    harness_tree = paths.eval_logs_dir / "run_evaluation" / config.run_id
    for model_dir in harness_tree.glob("*"):
        for instance_dir in model_dir.iterdir():
            if not instance_dir.is_dir():
                continue
            target = paths.eval_logs_dir / instance_dir.name
            instance_dir.replace(target)
            report = target / "report.json"
            if report.is_file():
                shutil.copy2(report, paths.eval_reports_dir / f"{instance_dir.name}.json")
    if harness_tree.parent.is_dir():
        shutil.rmtree(harness_tree.parent)

    summaries = sorted(paths.eval_dir.glob(f"*.{config.run_id}.json"))
    if summaries:
        summaries[0].replace(paths.eval_reports_dir / "summary.json")


def validate_eval(paths: RunPaths) -> None:
    """The one artifact every later step depends on is the batch summary."""
    if not (paths.eval_reports_dir / "summary.json").is_file():
        raise RuntimeError(
            f"harness finished but no summary landed in {paths.eval_reports_dir}"
        )


def run_eval(config: RunConfig, paths: RunPaths) -> dict:
    """Run the harness; reshape + validate outputs. Returns a small result
    dict for the CLI to print. Harness output goes to stderr (stdout is the
    CLI's one-line JSON contract)."""
    subprocess.run(
        build_eval_command(config, paths),
        cwd=paths.eval_dir,
        check=True,
        stdout=sys.stderr,
    )
    relocate_eval_outputs(config, paths)
    validate_eval(paths)
    return {
        "run_id": config.run_id,
        "summary": str(paths.eval_reports_dir / "summary.json"),
    }
