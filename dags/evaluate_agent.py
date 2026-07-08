"""Layer 4: THE pipeline DAG — one parameterized button for a full evaluation.

Architecture rules this file lives by (PLAN §2):
- imports ONLY pipeline.config from the project (stdlib-only Layer 0);
  heavy work happens behind `python -m pipeline.cli <step>` subprocesses in
  the project venv — the orchestrator env never needs project deps.
- zero hard-coded experiment values: the trigger form is generated from
  PARAM_DEFAULTS (SPEC C1/1.1.3).
- tasks exchange only the run_id via XCom; all data lives in runs/<run-id>/.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import PARAM_DEFAULTS  # noqa: E402 (needs sys.path above)

# JSON-schema types for the trigger form, derived from each default's type.
_PARAM_TYPES = {str: "string", int: "integer", float: "number"}

# Timeouts are env-tunable rather than param-scaled (execution_timeout is
# fixed at parse time). Defaults follow BREAKDOWN "Pipeline Runtime":
# agent ≈ ceil(n/workers) × 30 min, eval ≈ ceil(n/workers) × 10 min + image
# pulls — sized here for smoke/graded batches with headroom.
_AGENT_TIMEOUT_MIN = int(os.environ.get("AGENT_TIMEOUT_MINUTES", "120"))
_EVAL_TIMEOUT_MIN = int(os.environ.get("EVAL_TIMEOUT_MINUTES", "90"))


def _cli(step: str, *args: str) -> dict:
    """Run one pipeline CLI step in the execution env.

    stderr inherits the task's log stream (live tool output in the UI);
    stdout is the CLI's one-line JSON contract, parsed and returned.
    """
    result = subprocess.run(
        ["uv", "run", "python", "-m", "pipeline.cli", step, *args],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


@dag(
    dag_id="evaluate_agent",
    description="mini-swe-agent over SWE-bench: agent → eval → metrics → tracking",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["swe-bench", "evaluation"],
    params={
        **{
            key: Param(default, type=_PARAM_TYPES[type(default)])
            for key, default in PARAM_DEFAULTS.items()
        },
        "run_id": Param(
            "",
            type="string",
            description="Optional explicit run id (reruns); generated when empty.",
        ),
    },
)
def evaluate_agent():
    @task(retries=0, execution_timeout=timedelta(minutes=1))
    def prepare_run(**context) -> str:
        params = context["params"]
        args: list[str] = []
        for key in PARAM_DEFAULTS:
            args += ["--" + key.replace("_", "-"), str(params[key])]
        if params["run_id"]:
            args += ["--run-id", params["run_id"]]
        return _cli("prepare-run", *args)["run_id"]

    @task(
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=_AGENT_TIMEOUT_MIN),
    )
    def run_agent(pipeline_run_id: str) -> str:
        _cli("run-agent", "--run-dir", f"runs/{pipeline_run_id}")
        return pipeline_run_id

    @task(
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=_EVAL_TIMEOUT_MIN),
    )
    def run_eval(pipeline_run_id: str) -> str:
        _cli("run-eval", "--run-dir", f"runs/{pipeline_run_id}")
        return pipeline_run_id

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def summarize_and_log(pipeline_run_id: str) -> dict:
        return _cli("summarize", "--run-dir", f"runs/{pipeline_run_id}")

    summarize_and_log(run_eval(run_agent(prepare_run())))


evaluate_agent()
