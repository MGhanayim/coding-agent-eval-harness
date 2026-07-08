"""Layer 2: run the mini-swe-agent batch — the "agent under test" step.

`build_agent_command()` is pure (returns argv, no side effects) so the exact
command a config produces is unit-testable. `run_agent()` owns the side
effects: subprocess, output relocation, validation.
"""
from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import sys

from pipeline.artifacts import RunPaths
from pipeline.config import RunConfig, collect_package_versions


def build_agent_command(config: RunConfig, paths: RunPaths) -> list[str]:
    """Translate a RunConfig into the `mini-extra swebench` argv.

    Mirrors scripts/mini-swe-bench-batch.sh, minus its repo-checkout --config
    path: `-c swebench.yaml` resolves to the copy packaged inside the
    installed mini-swe-agent — reference-only upstreams (SPEC C4).

    cost_limit is ALWAYS passed explicitly. The packaged swebench.yaml sets
    its own agent.cost_limit (3.0), so omitting the override would silently
    re-enable a $3 ceiling; mini-swe-agent's check is `0 < cost_limit <= cost`,
    so an explicit 0 genuinely disables it. (With providers litellm has no
    pricing for — e.g. Kimi via Nebius — tracked cost stays $0 and no ceiling
    triggers either way; the step limit is the real bound there.)
    """
    return [
        "mini-extra", "swebench",
        "--subset", config.subset,
        "--split", config.split,
        "--model", config.model,
        "--slice", config.task_slice,
        "--workers", str(config.workers),
        "-o", str(paths.trajectories_dir),
        "-c", "swebench.yaml",
        "-c", f"agent.cost_limit={config.cost_limit}",
    ]


def refresh_package_versions(config: RunConfig, paths: RunPaths) -> RunConfig:
    """Re-record package versions from THIS environment when config.json
    carries "unknown" values. prepare-run may execute where the agent/harness
    packages aren't installed (the bare Airflow image under compose); the
    execution env is the authority, so provenance is corrected here before
    anything downstream consumes it (SPEC 2.2)."""
    if all(v != "unknown" for v in config.package_versions.values()):
        return config
    fixed = dataclasses.replace(config, package_versions=collect_package_versions())
    paths.config_path.write_text(fixed.to_json())
    return fixed


def relocate_preds(paths: RunPaths) -> None:
    """Copy preds.json from trajectories/ up to run-agent/preds.json.

    mini-extra writes predictions *inside* its -o directory; the SPEC 2.1
    contract wants them beside trajectories/ (PLAN §8 W1). It is a COPY,
    not a move: mini-extra's resume/skip logic reads <output>/preds.json,
    so removing it would turn every re-run into a full-price redo of the
    whole batch.
    """
    source = paths.trajectories_dir / "preds.json"
    if source.exists():
        shutil.copy2(source, paths.preds_path)


def validate_preds(paths: RunPaths) -> int:
    """Fail loudly unless preds.json exists and carries at least one
    non-empty patch. An exit-0 agent batch with nothing usable inside must
    fail the pipeline task here, not surface as a confusing eval error.
    Returns the number of non-empty patches."""
    if not paths.preds_path.is_file():
        raise RuntimeError(f"agent batch produced no predictions file at {paths.preds_path}")
    preds: dict = json.loads(paths.preds_path.read_text())
    if not preds:
        raise RuntimeError("predictions file is empty — no instances were attempted")
    patches = sum(1 for entry in preds.values() if entry.get("model_patch"))
    if patches == 0:
        raise RuntimeError(
            f"all {len(preds)} predictions have empty patches — nothing to evaluate"
        )
    return patches


def run_agent(config: RunConfig, paths: RunPaths) -> dict:
    """Run the agent batch; relocate + validate outputs. Returns a small
    result dict for the CLI to print. Tool output is routed to stderr so the
    CLI's stdout stays a parseable one-line JSON contract."""
    config = refresh_package_versions(config, paths)
    env = {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}
    subprocess.run(
        build_agent_command(config, paths),
        env=env,
        check=True,
        stdout=sys.stderr,
    )
    relocate_preds(paths)
    patches = validate_preds(paths)
    return {
        "run_id": config.run_id,
        "predictions": str(paths.preds_path),
        "non_empty_patches": patches,
    }
