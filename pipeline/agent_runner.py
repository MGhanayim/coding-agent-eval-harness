"""Layer 2: run the mini-swe-agent batch — the "agent under test" step.

`build_agent_command()` is pure (returns argv, no side effects) so the exact
command a config produces is unit-testable. `run_agent()` owns the side
effects: subprocess, output relocation, validation.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

from pipeline.artifacts import RunPaths
from pipeline.config import RunConfig


def build_agent_command(config: RunConfig, paths: RunPaths) -> list[str]:
    """Translate a RunConfig into the `mini-extra swebench` argv.

    Mirrors scripts/mini-swe-bench-batch.sh, minus its repo-checkout --config
    path: with no -c flag, mini-extra falls back to the swebench.yaml packaged
    inside the installed mini-swe-agent — reference-only upstreams (SPEC C4).

    cost_limit rides in as a config override (the batch CLI has no dedicated
    flag). 0 means "no cost ceiling" and is omitted entirely. Note: with
    providers litellm has no pricing table for (e.g. Kimi via Nebius), cost
    tracking reports $0 and the ceiling never triggers — the step limit is
    the real bound (see BREAKDOWN "Pipeline Runtime").
    """
    command = [
        "mini-extra", "swebench",
        "--subset", config.subset,
        "--split", config.split,
        "--model", config.model,
        "--slice", config.task_slice,
        "--workers", str(config.workers),
        "-o", str(paths.trajectories_dir),
    ]
    if config.cost_limit > 0:
        command += ["-c", "swebench.yaml", "-c", f"agent.cost_limit={config.cost_limit}"]
    return command


def relocate_preds(paths: RunPaths) -> None:
    """Move preds.json out of trajectories/ up to run-agent/preds.json.

    mini-extra writes predictions *inside* its -o directory; the SPEC 2.1
    contract wants them beside trajectories/, not among them (PLAN §8 W1).
    """
    misplaced = paths.trajectories_dir / "preds.json"
    if misplaced.exists():
        misplaced.replace(paths.preds_path)


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
