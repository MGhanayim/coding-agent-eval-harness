"""Layer 4: THE pipeline DAG — one parameterized button for a full evaluation.

Architecture rules this file lives by (PLAN §2):
- imports ONLY pipeline.config from the project (stdlib-only Layer 0);
  heavy work happens behind `python -m pipeline.cli <step>` — the
  orchestrator env never needs project deps.
- zero hard-coded experiment values: the trigger form is generated from
  PARAM_DEFAULTS (SPEC C1/1.1.3).
- tasks exchange only the run_id via XCom; all data lives in runs/<run-id>/.

Executor switch (PLAN §10): EXECUTION_MODE=subprocess (default, easy mode)
runs every step as `uv run python -m pipeline.cli ...` on the host;
EXECUTION_MODE=docker (production mode, Block H) runs the heavy steps
(run_agent, run_eval) as DockerOperator containers over the SAME CLI —
the command is the contract, the executor is an implementation detail.
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

# subprocess (easy) or docker (production, requires the Docker provider).
EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "subprocess")
TASK_IMAGE = os.environ.get("TASK_IMAGE", "coding-agent-eval-harness:latest")
# Host path of this repo — bind-mount sources must be HOST paths even when
# Airflow itself runs in a container (PLAN §8 W3).
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", str(PROJECT_ROOT))
TASK_NETWORK_MODE = os.environ.get("TASK_NETWORK_MODE", "bridge")

# Env the task containers need (secrets + service endpoints); values come
# from the environment Airflow itself runs with — never from code (SPEC C2).
TASK_ENV_KEYS = (
    "NEBIUS_API_KEY",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_EXPERIMENT_NAME",
    "AWS_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "RUNS_BUCKET",
)

# Timeouts are env-tunable rather than param-scaled (execution_timeout is
# fixed at parse time). Defaults follow BREAKDOWN "Pipeline Runtime":
# agent ≈ ceil(n/workers) × 30 min, eval ≈ ceil(n/workers) × 10 min + image
# pulls — sized here for smoke/graded batches with headroom.
_AGENT_TIMEOUT = timedelta(minutes=int(os.environ.get("AGENT_TIMEOUT_MINUTES", "120")))
_EVAL_TIMEOUT = timedelta(minutes=int(os.environ.get("EVAL_TIMEOUT_MINUTES", "90")))


def _cli(step: str, *args: str) -> dict:
    """Run one pipeline CLI step as a subprocess in the project env.

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


def _docker_step(task_id: str, step: str, retries: int, timeout: timedelta):
    """The same CLI step, executed inside the pinned task image.

    The container reaches the run dir through a bind mount of the HOST
    runs/ path, and run_eval reaches the Docker daemon through the socket
    mount — per-instance eval containers are *siblings* of the task
    container, not children (docker-out-of-docker).
    """
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    run_dir = "runs/{{ ti.xcom_pull(task_ids='prepare_run') }}"
    return DockerOperator(
        task_id=task_id,
        image=TASK_IMAGE,
        command=["python", "-m", "pipeline.cli", step, "--run-dir", run_dir],
        environment={key: os.environ.get(key, "") for key in TASK_ENV_KEYS},
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/runs",
                target="/mlops-assignment/runs",
                type="bind",
            ),
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            ),
        ],
        network_mode=TASK_NETWORK_MODE,
        mount_tmp_dir=False,
        auto_remove="success",
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timeout,
    )


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
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def summarize_and_log(pipeline_run_id: str) -> dict:
        return _cli("summarize", "--run-dir", f"runs/{pipeline_run_id}")

    if EXECUTION_MODE == "docker":
        # All non-trivial steps in containers: prepare-run is stdlib-only so
        # it runs on the orchestrator's python; summarize needs mlflow/boto3,
        # which live in the task image, not the Airflow image.
        run_id_xcom = prepare_run()
        agent = _docker_step("run_agent", "run-agent", retries=1, timeout=_AGENT_TIMEOUT)
        evaluate = _docker_step("run_eval", "run-eval", retries=1, timeout=_EVAL_TIMEOUT)
        summary = _docker_step(
            "summarize_and_log", "summarize", retries=2, timeout=timedelta(minutes=5)
        )
        run_id_xcom >> agent >> evaluate >> summary
    else:

        @task(
            retries=1,
            retry_delay=timedelta(minutes=2),
            execution_timeout=_AGENT_TIMEOUT,
        )
        def run_agent(pipeline_run_id: str) -> str:
            _cli("run-agent", "--run-dir", f"runs/{pipeline_run_id}")
            return pipeline_run_id

        @task(
            retries=1,
            retry_delay=timedelta(minutes=2),
            execution_timeout=_EVAL_TIMEOUT,
        )
        def run_eval(pipeline_run_id: str) -> str:
            _cli("run-eval", "--run-dir", f"runs/{pipeline_run_id}")
            return pipeline_run_id

        summarize_and_log(run_eval(run_agent(prepare_run())))


evaluate_agent()
