"""Layer 4: THE pipeline DAG — one parameterized button for a full evaluation.

Architecture rules this file lives by (PLAN §2):
- imports ONLY pipeline.config from the project (stdlib-only Layer 0);
  heavy work happens behind `python -m pipeline.cli <step>` — the
  orchestrator env never needs project deps.
- zero hard-coded experiment values: the trigger form is generated from
  PARAM_DEFAULTS (SPEC C1/1.1.3).
- tasks exchange only {run_id, run_dir} via XCom; all data lives in the
  run dir the CLI reports (RUNS_ROOT-aware — the DAG never derives paths).

Executor switch (PLAN §10): EXECUTION_MODE=subprocess (default, easy mode)
runs every step as `python -m pipeline.cli ...` on the host;
EXECUTION_MODE=docker (production mode, Block H) runs the heavy steps
as DockerOperator containers over the SAME CLI — the command is the
contract, the executor is an implementation detail.
"""
from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import PARAM_DEFAULTS, load_env_file  # noqa: E402

# Make the project .env visible to the orchestrator (standalone Airflow does
# not source it): docker-mode task env and subprocess-mode children both
# inherit from this process. Under compose the env_file already provides
# everything; override=False keeps that authoritative.
load_env_file(PROJECT_ROOT / ".env")

# JSON-schema types for the trigger form, derived from each default's type.
_PARAM_TYPES = {str: "string", int: "integer", float: "number"}

# `or`-defaults everywhere: compose interpolation turns unset .env variables
# into present-but-EMPTY env vars, which os.environ.get(k, default) misses.
EXECUTION_MODE = os.environ.get("EXECUTION_MODE") or "subprocess"
TASK_IMAGE = os.environ.get("TASK_IMAGE") or "coding-agent-eval-harness:latest"
# Host path of this repo — bind-mount sources must be HOST paths even when
# Airflow itself runs in a container (PLAN §8 W3).
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR") or str(PROJECT_ROOT)
# Host path of the runs root (only diverges from <project>/runs when
# RUNS_ROOT is customized); also handed to summarize so MLflow provenance
# records a path that exists on the host, not in a container.
HOST_RUNS_DIR = os.environ.get("HOST_RUNS_DIR") or f"{HOST_PROJECT_DIR}/runs"
TASK_NETWORK_MODE = os.environ.get("TASK_NETWORK_MODE") or "bridge"

# How the light steps invoke the CLI. Standalone (host): through uv and the
# project venv. Compose: the Airflow container has no uv, but prepare-run is
# stdlib-only, so its bare python suffices (PIPELINE_PYTHON="python").
PIPELINE_PYTHON = (os.environ.get("PIPELINE_PYTHON") or "uv run python").split()

# Env the task containers need (secrets + service endpoints); values come
# from the environment Airflow itself runs with — never from code (SPEC C2).
# Only NON-EMPTY values are forwarded: sending "" would defeat the in-code
# defaults (e.g. bucket_name(), experiment_name()) inside the container.
TASK_ENV_KEYS = (
    "NEBIUS_API_KEY",
    "HF_TOKEN",
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
_AGENT_TIMEOUT = timedelta(minutes=int(os.environ.get("AGENT_TIMEOUT_MINUTES") or 120))
_EVAL_TIMEOUT = timedelta(minutes=int(os.environ.get("EVAL_TIMEOUT_MINUTES") or 90))
# prepare-run is stdlib-light, but `uv run` may bootstrap the whole venv on
# a cold cache (fresh VM / changed lockfile) — budget for that, retries=0.
_PREPARE_TIMEOUT = timedelta(minutes=5)
_RETRY_DELAY = timedelta(minutes=2)

# One retry/timeout spec consumed by BOTH executor branches, so the mode we
# develop in and the mode production runs can't drift apart.
STEP_POLICY: dict[str, tuple[int, timedelta]] = {
    "run_agent": (1, _AGENT_TIMEOUT),
    "run_eval": (1, _EVAL_TIMEOUT),
    "summarize_and_log": (2, timedelta(minutes=5)),
}


def _cli(step: str, *args: str) -> dict:
    """Run one pipeline CLI step as a subprocess in the execution env.

    The child gets its own process group, and any failure — including
    Airflow's execution_timeout — kills the WHOLE group: without that, the
    grandchild batch (mini-extra / the harness) would survive the task and
    race its own retry over the same run dir.

    stderr inherits the task's log stream (live tool output in the UI);
    stdout is the CLI's one-line JSON contract, parsed and returned.
    """
    process = subprocess.Popen(
        [*PIPELINE_PYTHON, "-m", "pipeline.cli", step, *args],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, _ = process.communicate()
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        raise
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, process.args)
    return json.loads(stdout.strip().splitlines()[-1])


def _docker_step(task_id: str, step: str):
    """The same CLI step, executed inside the pinned task image.

    The container reaches the run dir through a bind mount of the HOST
    runs/ path, and run_eval reaches the Docker daemon through the socket
    mount — per-instance eval containers are *siblings* of the task
    container, not children (docker-out-of-docker).
    """
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    retries, timeout = STEP_POLICY[task_id]
    environment = {
        key: value
        for key in TASK_ENV_KEYS
        if (value := os.environ.get(key))
    }
    environment["HOST_RUNS_DIR"] = HOST_RUNS_DIR
    run_dir = "runs/{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}"
    return DockerOperator(
        task_id=task_id,
        image=TASK_IMAGE,
        command=["python", "-m", "pipeline.cli", step, "--run-dir", run_dir],
        environment=environment,
        mounts=[
            Mount(
                source=HOST_RUNS_DIR,
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
        retry_delay=_RETRY_DELAY,
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
            description=(
                "Optional explicit run id; generated when empty. Must be "
                "unused — for reruns pick a fresh suffix (e.g. <old-id>-rerun)."
            ),
        ),
    },
)
def evaluate_agent():
    @task(retries=0, execution_timeout=_PREPARE_TIMEOUT)
    def prepare_run(**context) -> dict:
        params = context["params"]
        args: list[str] = []
        for key in PARAM_DEFAULTS:
            args += ["--" + key.replace("_", "-"), str(params[key])]
        if params["run_id"]:
            args += ["--run-id", params["run_id"]]
        # {"run_id": ..., "run_dir": ...} — run_dir is the CLI's authoritative,
        # RUNS_ROOT-aware location; the DAG never builds runs/ paths itself.
        return _cli("prepare-run", *args)

    if EXECUTION_MODE == "docker":
        # Heavy + dep-heavy steps in containers: prepare-run is stdlib-only
        # so it runs on the orchestrator's python; summarize needs
        # mlflow/boto3, which live in the task image, not the Airflow image.
        prepared = prepare_run()
        agent = _docker_step("run_agent", "run-agent")
        evaluate = _docker_step("run_eval", "run-eval")
        summary = _docker_step("summarize_and_log", "summarize")
        prepared >> agent >> evaluate >> summary
    else:

        def _step(name: str, step: str):
            retries, timeout = STEP_POLICY[name]

            @task(
                task_id=name,
                retries=retries,
                retry_delay=_RETRY_DELAY,
                execution_timeout=timeout,
            )
            def run(prepared: dict) -> dict:
                _cli(step, "--run-dir", prepared["run_dir"])
                return prepared

            return run

        prepared = prepare_run()
        agent = _step("run_agent", "run-agent")(prepared)
        evaluate = _step("run_eval", "run-eval")(agent)
        _step("summarize_and_log", "summarize")(evaluate)


evaluate_agent()
