"""Layer 2: judge the agent's patches with the SWE-bench harness.

The harness writes relative to its cwd: `logs/run_evaluation/<run_id>/
<model-slug>/<instance>/...` plus a `<model-slug>.<run_id>.json` summary
(and, when it builds images locally, `logs/build_images/...`). We run it
with cwd=run-eval/ so everything lands inside the run dir, then reshape to
the SPEC 2.1 contract:

    run-eval/logs/<instance_id>/{report.json, patch.diff, test_output.txt, ...}
    run-eval/reports/{summary.json, <instance_id>.json}

Every step here is retry-idempotent: an Airflow retry re-runs the harness
and relocation over a partially-populated run dir, so relocation must
overwrite, never collide (PLAN §6).
"""
from __future__ import annotations

import shutil
import subprocess
import sys

from pipeline.artifacts import RunPaths
from pipeline.config import RunConfig


def model_slug(config: RunConfig) -> str:
    """The harness's directory-safe model name (slashes become __)."""
    return config.model.replace("/", "__")


def summary_source_path(config: RunConfig, paths: RunPaths):
    """Exact path of the harness batch summary — computed, not globbed,
    so a stale file from an earlier attempt can never be picked up."""
    return paths.eval_dir / f"{model_slug(config)}.{config.run_id}.json"


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


def cleanup_stale_containers(config: RunConfig) -> None:
    """Best-effort removal of leftover `sweb.eval.<instance>.<run_id>`
    containers from a killed previous attempt. Without this, the retry's
    container creation hits 409 Conflict, the harness books those instances
    as errors, and the run's metrics are silently corrupted."""
    try:
        listing = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"name={config.run_id}"],
            capture_output=True,
            text=True,
            check=True,
        )
        stale = listing.stdout.split()
        if stale:
            print(f"removing {len(stale)} stale eval container(s)", file=sys.stderr)
            subprocess.run(
                ["docker", "rm", "-f", *stale], check=False, stdout=sys.stderr
            )
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"stale-container cleanup skipped: {error}", file=sys.stderr)


def _strip_symlinks(directory) -> None:
    """Drop symlinks (e.g. the harness's absolute `image_build_dir` link):
    they dangle after upload/download and break folder reconstruction."""
    for entry in directory.iterdir():
        if entry.is_symlink():
            entry.unlink()


def relocate_eval_outputs(config: RunConfig, paths: RunPaths) -> None:
    """Reshape raw harness output into the run-dir contract (idempotent).

    Flattens logs/run_evaluation/<run_id>/<model-slug>/<instance>/ down to
    logs/<instance>/ (overwriting leftovers from a previous attempt), moves
    the summary to reports/summary.json, copies each per-instance report.json
    to reports/<instance>.json, and parks any local image-build logs outside
    logs/ so the SPEC 2.1 shape holds (PLAN §8 W2).
    """
    harness_tree = paths.eval_logs_dir / "run_evaluation" / config.run_id
    for model_dir in harness_tree.glob("*"):
        if not model_dir.is_dir():
            continue
        for instance_dir in model_dir.iterdir():
            if not instance_dir.is_dir():
                continue
            target = paths.eval_logs_dir / instance_dir.name
            if target.exists():
                shutil.rmtree(target)
            _strip_symlinks(instance_dir)
            instance_dir.replace(target)
            report = target / "report.json"
            if report.is_file():
                shutil.copy2(report, paths.eval_reports_dir / f"{instance_dir.name}.json")
    if harness_tree.parent.is_dir():
        shutil.rmtree(harness_tree.parent)

    build_logs = paths.eval_logs_dir / "build_images"
    if build_logs.is_dir():
        target = paths.eval_dir / "build_images"
        if target.exists():
            shutil.rmtree(target)
        build_logs.replace(target)

    summary = summary_source_path(config, paths)
    if summary.is_file():
        summary.replace(paths.eval_reports_dir / "summary.json")


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
    cleanup_stale_containers(config)
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
