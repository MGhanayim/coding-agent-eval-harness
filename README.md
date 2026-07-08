# Coding-Agent Eval Harness

> An end-to-end MLOps pipeline that runs coding agents against SWE-bench and turns every
> experiment into a reproducible, comparable, durable artifact — one Airflow trigger from
> config to metrics.

![Python](https://img.shields.io/badge/python-3.12+-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.2-017CEE?logo=apacheairflow)
![MLflow](https://img.shields.io/badge/MLflow-3.14-0194E2?logo=mlflow)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker)

> 🚧 **In development** — built block by block; progress in [CLAUDE.md](CLAUDE.md).
> This notice is removed when the pipeline is fully deployed.

## Demo

<!-- TODO(Block K): screenshots/airflow_dag.png + mlflow_runs.png side by side -->
*Screenshots coming after the first full deployment (see `screenshots/`).*

## What This Project Demonstrates

- **Pipeline orchestration** — a parameterized Airflow DAG (`prepare_run → run_agent →
  run_eval → summarize_and_log`) with retries, timeouts, and zero hard-coded experiment values
- **Experiment tracking** — every run's params, metrics, and artifact URIs logged to MLflow
  and comparable across models/configs
- **Reproducibility engineering** — each run emits a self-describing `runs/<run-id>/` tree
  (config, trajectories, predictions, eval reports, metrics, manifest) uploaded to S3
- **Execution isolation** — agent and evaluation steps run via `DockerOperator` in a pinned
  image; the SWE-bench harness spawns per-instance test containers
- **Clean layered architecture** — orchestration and execution environments strictly
  separated behind one CLI contract (see [PLAN.md](PLAN.md))

## Quick Start

```bash
git clone <repo-url> && cd coding-agent-eval-harness
uv sync
cp .env.example .env          # add your NEBIUS_API_KEY

# Easy mode: standalone Airflow
bash run-airflow-standalone.sh          # UI at http://localhost:8080

# Production mode: full stack
docker compose up -d                    # Airflow + MLflow + MinIO
```

Trigger the `evaluate_agent` DAG with e.g. `task_slice=0:3, cost_limit=0` for a 3-instance
smoke run.

## Architecture

Airflow orchestrates; all real work runs behind `python -m pipeline.cli <step>` in an
isolated execution environment (project venv locally, Docker image in production). Each run
writes a reproducible artifact tree, ships it to object storage, and registers itself in
MLflow. Full diagrams, dependency rules, and walkthroughs: [PLAN.md](PLAN.md).

## Tech Stack

- **Airflow 3.2** — orchestration (standalone for dev, docker-compose for deployment)
- **mini-swe-agent + SWE-bench** — the agent under test and the test-based judge
- **MLflow 3.14** — experiment tracking and run comparison
- **MinIO / S3** — durable artifact storage (endpoint-swappable to any S3-compatible store)
- **uv + Docker** — pinned, reproducible environments everywhere

## Example Usage

<!-- TODO(Block K): real run — params in, metrics out, MLflow compare screenshot -->

```text
Trigger: split=test subset=verified workers=4 task_slice=0:3
Result:  3 submitted · 3 completed · 1 resolved · resolve_rate 0.33
         runs/20260702T142530__verified__0-3/ → s3://runs/... → MLflow run
```

## Project Structure

See [PLAN.md §3](PLAN.md) for the annotated tree and layer assignments.

## License

MIT
