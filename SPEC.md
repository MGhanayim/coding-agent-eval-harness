# SPEC.md — Assignment Requirements & Acceptance Criteria

> Source: Nebius Academy course *AI Performance Engineering*, MLOps module, lecture #6
> "End-to-end ML pipeline" — home assignment by Simon Karasik
> (starter repo: `minotru/mlops-assignment-e2e-ml-pipeline`, preserved locally as `ASSIGNMENT.md`).
> Due: not stated in the source.
> Scope chosen: **full production-style** (all areas, including DockerOperator, Docker Compose, and S3 upload).

## Overview

Turn the provided ad-hoc shell scripts — which run [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent)
on SWE-bench instances and evaluate the resulting patches with the
[SWE-bench harness](https://github.com/swe-bench/SWE-bench) — into a configurable, observable,
durable **Airflow pipeline**: `prepare_run → run_agent → run_eval → summarize_and_log`,
with a structured `runs/<run-id>/` artifact tree, artifacts uploaded to S3-compatible object
storage, and every run's params/metrics/artifact-URIs logged to **MLflow**.

## Dataset / Inputs

| Input | Value | Notes |
|---|---|---|
| Benchmark dataset | `princeton-nlp/SWE-bench_Verified` (HuggingFace) | 500 instances; selected via `--subset verified --split test`; fetched automatically by mini-swe-agent / swebench |
| Agent | `mini-swe-agent` 2.4.1 (`mini-extra swebench` / `swebench-single` CLIs) | pinned in `uv.lock` |
| Evaluator | `swebench` 4.1.0 (`python -m swebench.harness.run_evaluation`) | runs per-instance Docker containers |
| LLM | `nebius/moonshotai/Kimi-K2.6` via Nebius Token Factory (default; must be a parameter) | needs `NEBIUS_API_KEY` |
| Sample outputs | `sample/` | reference for trajectory / `preds.json` / eval report formats |
| Starter DAG | `dags/mini-swe-bench-single.py` | re-implements `scripts/mini-swe-bench-single.sh` |

## Constraints (Apply to All Tasks)

| # | Constraint |
|---|---|
| C1 | No hard-coded experiment values — everything experiment-specific comes from Airflow params |
| C2 | Secrets never committed; `.env` (gitignored) + `.env.example` template |
| C3 | Agent/eval steps get sensible **retries and timeouts** (assignment Phase 3 requirement) |
| C4 | Upstream repos (mini-swe-agent, SWE-bench) are reference material only — not vendored into the pipeline |
| C5 | Runtime outputs (`runs/`, `trajectories/`, `logs/`, `mlruns/`, MinIO data) are gitignored; only small samples/manifests may be committed |
| C6 | MLflow must be reachable from the execution environment and used **by the DAG** (not logged by hand) |

## Area 1 — Configurable Airflow DAG (35%)

### 1.1 Parameters

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 1.1.1 | Required params: `split`, `subset`, `workers` | Given the Airflow UI trigger form, when a user triggers the DAG, then all three are editable params with sane defaults (e.g. `test`, `verified`, `4`) |
| 1.1.2 | Optional params: `model`, `task_slice`, `run_id`, `cost_limit` | Given the trigger form, when left blank/default, then the pipeline still runs (auto-generated `run_id`, default model/slice/cost-limit) |
| 1.1.3 | No hard-coded experiment values | Given the DAG source, when searched for model names, slices, splits, or worker counts, then they appear only as param defaults — never inside task logic |

### 1.2 Task structure

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 1.2.1 | `prepare_run` task | Reads params, resolves the run config, creates `runs/<run-id>/` and writes `config.json` |
| 1.2.2 | `run_agent` task | Runs mini-swe-agent batch with the run's params; trajectories + `preds.json` land in `runs/<run-id>/run-agent/` |
| 1.2.3 | `run_eval` task | Runs the SWE-bench harness on that `preds.json`; logs + reports land in `runs/<run-id>/run-eval/` |
| 1.2.4 | `summarize_and_log` task | Parses eval reports, writes `metrics.json` + `manifest.json`, logs to MLflow |
| 1.2.5 | Reliable UI triggering | Given a running Airflow instance, when the DAG is triggered twice with different params, then both runs complete independently with separate run dirs |

**Test queries / examples:** trigger with `split=test, subset=verified, workers=4, task_slice=0:3, cost_limit=0` — a 3-instance smoke batch matching `scripts/mini-swe-bench-batch.sh`.

## Area 2 — Artifact structure & reproducibility (20%)

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 2.1 | Structured run tree | Every run produces: `runs/<run-id>/{config.json, run-agent/{preds.json, trajectories/}, run-eval/{logs/, reports/}, metrics.json, manifest.json}` |
| 2.2 | Run is self-describing | Given only a `runs/<run-id>/` folder, a teammate can reconstruct: input tasks (dataset/split/subset/slice), configuration, trajectories, predictions, eval logs, and metrics |
| 2.3 | `manifest.json` | Points to the important files and records where full artifacts live (local paths + remote URI) |
| 2.4 | S3 upload (extra credit within this area) | The run folder (or a compressed copy) is uploaded to S3-compatible object storage; the URI is recorded in `manifest.json` and logged to MLflow |

**Test:** copy one completed `runs/<run-id>/` to a clean location and answer, from files alone: which model? which slice? what resolve rate? where do full artifacts live?

## Area 3 — MLflow tracking (15%)

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 3.1 | Params logged | `split`, `subset`, `workers`, `model`, `task_slice`, `cost_limit`, `run_id` visible on the MLflow run |
| 3.2 | Metrics logged | At minimum: submitted / completed / resolved instance counts and resolve rate, parsed from the eval report |
| 3.3 | Artifact reference logged | The local path and/or S3 URI of `runs/<run-id>/` is attached to the MLflow run |
| 3.4 | Runs comparable | Given ≥2 completed evaluations, the MLflow UI compares their params & metrics side by side |
| 3.5 | Reachable + used by the DAG | MLflow server reachable from the execution environment; logging happens inside `summarize_and_log`, not manually |

## Area 4 — Execution isolation (10%)

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 4.1 | `DockerOperator` for heavy steps | `run_agent` and `run_eval` execute inside containers built from the provided `Dockerfile` (production-style path) |
| 4.2 | Repeatable environment | The image pins deps via `uv.lock`; `docker build` + documented env vars is all that's needed to reproduce the execution environment |
| 4.3 | Eval can spawn containers | The SWE-bench harness inside the container can start per-instance evaluation containers (Docker socket mounted) |

*Fallback allowed by grader: a clear standalone-Airflow implementation without DockerOperator still earns most of this area — we target the full version.*

## Area 5 — Docker Compose deployment (10%)

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 5.1 | `docker-compose.yaml` | Brings up Airflow, MLflow, and MinIO (object storage) on the VM with one command |
| 5.2 | Documented setup | Required env vars listed in `.env.example`; startup steps in README/REPORT |
| 5.3 | Services wired | The DAG running under Compose reaches MLflow and MinIO by service name; compose supports the pipeline rather than becoming the main point |

## Area 6 — Report & reproducibility (10%)

| ID | Requirement | Acceptance Criteria |
|----|-------------|---------------------|
| 6.1 | `REPORT.md` exists | Covers: architecture, how to trigger the DAG, artifact layout, MLflow link/screenshot, one completed evaluation, and how to rerun by `run-id` |
| 6.2 | Evidence of a completed run | At least one full `run-agent → run-eval → log` cycle with real metrics |
| 6.3 | Screenshots (production-style deliverables) | `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png`, `screenshots/object_storage_artifacts.png` |

## Deliverables checklist (from the assignment's "Final Deliverables")

Minimum working submission:
- `dags/evaluate_agent.py` (or updated DAG) with the four tasks
- Airflow params: `split`, `subset`, `workers` (+ `model`, `task_slice`, `run_id`, `cost_limit`)
- Wrapper code so the DAG runs mini-swe-agent with DAG-provided params → `runs/<run-id>/run-agent/`
- Wrapper code so the DAG evaluates `preds.json` → `runs/<run-id>/run-eval/`
- A sample `runs/<run-id>/` folder or manifest committed
- An MLflow run with params, metrics, `run_id`, artifact path/URI
- `REPORT.md`

Production-style additions:
- `Dockerfile` (provided) used by `DockerOperator` tasks
- `docker-compose.yaml` for Airflow + MLflow (+ MinIO)
- `.env.example` covering Airflow, MLflow, object storage, and inference credentials
- S3/Object Storage upload of full run artifacts
- The three screenshots above

## Grading Summary

| Area | Weight |
|------|-------:|
| 1. Configurable Airflow DAG | 35% |
| 2. Artifact structure & reproducibility (S3 = extra credit within) | 20% |
| 3. MLflow tracking | 15% |
| 4. Execution isolation (DockerOperator) | 10% |
| 5. Docker Compose deployment | 10% |
| 6. Report & reproducibility | 10% |
| **Total** | **100%** |

> Grader's stated philosophy: *"A weak result with excellent provenance and analysis is better
> than a pasted number nobody can reproduce."* Engineering judgment and traceability over metrics.

## Verification Checklist

Pre-submission — every box maps to an ID above:

- [ ] 1.1.1 DAG exposes `split`, `subset`, `workers` params with defaults
- [ ] 1.1.2 DAG exposes `model`, `task_slice`, `run_id`, `cost_limit`; blank values auto-resolve
- [ ] 1.1.3 No experiment values hard-coded outside param defaults (grep verified)
- [ ] 1.2.1–1.2.4 Four tasks exist and are named/ordered `prepare_run → run_agent → run_eval → summarize_and_log`
- [ ] 1.2.5 Two UI-triggered runs with different params both succeed with separate run dirs
- [ ] 2.1 Run tree matches the required shape exactly
- [ ] 2.2 A teammate can reconstruct the run from the folder alone (dry-run passed on an S3-downloaded copy)
- [ ] 2.3 `manifest.json` lists key files + remote artifact URI
- [ ] 2.4 Run folder uploaded to MinIO/S3; URI in manifest + MLflow
- [ ] 3.1–3.3 MLflow run has params, metrics, artifact reference
- [ ] 3.4 Two runs comparable in MLflow UI (3 runs logged: rates 1.0 / 0.5 / 1.0)
- [ ] 3.5 Logging happens inside `summarize_and_log`
- [ ] 4.1–4.3 `run_agent`/`run_eval` run via DockerOperator; eval spawns per-instance sibling containers (observed live under compose)
- [ ] 5.1–5.3 `docker compose up` brings up Airflow + MLflow + MinIO; DAG reaches both by service name
- [ ] 6.1 `REPORT.md` complete (drafted — final numbers + screenshots after the VM run, Block J)
- [ ] 6.2 At least one real completed evaluation (several: local CLI, standalone-Airflow, and compose runs with real metrics)
- [ ] 6.3 Three screenshots committed (Block J, on the VM)
- [ ] C1–C6 All cross-cutting constraints hold (grep for hard-codes; `.gitignore` covers secrets + runtime output; retries/timeouts set per PLAN §6)
