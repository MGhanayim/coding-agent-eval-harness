# PLAN.md — Architecture

## 1. Context

This project turns ad-hoc coding-agent evaluation scripts (mini-swe-agent → SWE-bench harness)
into a configurable, durable Airflow pipeline with MLflow tracking and S3 artifact storage.
Requirements live in [SPEC.md](SPEC.md); the implementation is organized into learning blocks in
[CLAUDE.md](CLAUDE.md). Stack: **Python 3.12 + uv**, **Airflow 3.2.x**, **MLflow 3.14.x**,
**MinIO** (S3-compatible, swappable for Nebius Object Storage — same boto3 API), **Docker /
docker-compose**. Development happens on macOS; real evaluation batches run on a Nebius Linux VM
(SWE-bench per-instance images are x86_64).

**The one decision everything else follows from:** Airflow standalone runs in an isolated
`uv tool` environment that does *not* contain the project's heavy dependencies (mini-swe-agent,
swebench, mlflow, boto3). So the orchestration environment and the execution environment are
kept strictly separate. All real work is exposed through one CLI —
`python -m pipeline.cli <step>` — and the DAG only *builds commands*. In easy mode the command
runs via `subprocess` in the project venv; in production mode the **same command** runs via
`DockerOperator` in the project image. Migrating to Docker is a swap of the execution layer,
not a rewrite.

## 2. Clean Architecture / Dependency Rules

Rule: **lower layers never import from higher layers.** The DAG (highest) may import only
Layer 0 (`pipeline/config.py`, stdlib-only) for param defaults; it reaches everything else
through the CLI + files (config.json in, artifacts out).

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 4 — Orchestration (Airflow env)                       │
│   dags/evaluate_agent.py                                    │
└──────────────┬─────────────────────────┬────────────────────┘
   imports (stdlib-only)     runs command (subprocess / DockerOperator)
               │                         │
               ▼                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Layer 3 — Entry point (execution env)                       │
│   pipeline/cli.py                                           │
├─────────────────────────────────────────────────────────────┤
│ Layer 2 — Services                                          │
│   agent_runner.py  evaluator.py  metrics.py                 │
│   storage.py  tracking.py                                   │
├─────────────────────────────────────────────────────────────┤
│ Layer 1 — Artifact contract                                 │
│   artifacts.py                                              │
├─────────────────────────────────────────────────────────────┤
│ Layer 0 — Configuration                                     │
│   config.py            (stdlib-only: dataclass + defaults)  │
└─────────────────────────────────────────────────────────────┘
                 imports point DOWN only ▼
```

Anti-pattern guarded against: the DAG must never import `tracking.py` or `storage.py`
(they import mlflow/boto3, which don't exist in the Airflow env — the DAG would fail to
*parse*). Param defaults live once, in `config.py`, imported by both the DAG and the CLI.

## 3. Project Structure

```
coding-agent-eval-harness/
├── ASSIGNMENT.md                 # original assignment text (gitignored reference)
├── SPEC.md · PLAN.md · CLAUDE.md · BREAKDOWN.md   # planning docs
├── README.md                     # portfolio-facing readme
├── REPORT.md                     # assignment writeup (Block K)
├── .env.example                  # non-secret env template
├── pyproject.toml / uv.lock      # project deps (execution env)
├── Dockerfile                    # task image (provided; extended to COPY pipeline/)
├── docker-compose.yaml           # Airflow + MLflow + MinIO (Block I)
├── run-airflow-standalone.sh     # easy-mode Airflow (provided)
├── dags/
│   ├── mini-swe-bench-single.py  # [L4] starter example, kept as reference
│   └── evaluate_agent.py         # [L4] THE pipeline DAG
├── pipeline/
│   ├── __init__.py
│   ├── config.py                 # [L0] RunConfig dataclass, param defaults, env access
│   ├── artifacts.py              # [L1] runs/<run-id>/ layout, path helpers, manifest
│   ├── agent_runner.py           # [L2] build + run mini-swe-agent batch command
│   ├── evaluator.py              # [L2] build + run SWE-bench harness command
│   ├── metrics.py                # [L2] parse eval reports → metrics dict
│   ├── storage.py                # [L2] upload run dir to S3/MinIO (boto3)
│   ├── tracking.py               # [L2] log params/metrics/URIs to MLflow
│   └── cli.py                    # [L3] `python -m pipeline.cli <step> --run-dir ...`
├── scripts/                      # original ad-hoc scripts (reference / smoke tests)
├── sample/                       # provided sample outputs (format reference)
├── screenshots/                  # deliverable evidence (Block J)
└── runs/                         # runtime output (gitignored; one sample manifest committed)
```

## 4. Import Graph (contract — no cycles)

```
dags/evaluate_agent.py ────────────────► pipeline/config.py
        (only Layer 0; via sys.path)          ▲  ▲  ▲
                                              │  │  │
pipeline/cli.py ──► agent_runner.py ──► artifacts.py ──► config.py
      │      │                                ▲
      ├────► evaluator.py ────────────────────┤
      ├────► metrics.py ──────────────────────┤
      ├────► storage.py ──────────────────────┤
      └────► tracking.py ─────────────────────────────► config.py
```

- `cli.py` is the only module that imports the services; services never import each other.
- Heavy imports (`mlflow`, `boto3`, `swebench`) appear only in Layer 2 modules.
- The subprocess/DockerOperator boundary between L4 and L3 is an *execution* edge, not an
  import edge — that's what makes the two deployment modes interchangeable.

## 5. High-Level System Architecture

```
                        ┌────────────────────────────┐
   trigger w/ params    │        AIRFLOW             │
  ─────────────────────►│  UI ──► scheduler ──► DAG  │
                        │      evaluate_agent        │
                        └─────┬──────────────────────┘
                              │ python -m pipeline.cli <step>
                              │ (subprocess │ DockerOperator + task image)
                              ▼
   ┌──────────────────────────────────────────────────────────┐
   │              EXECUTION ENV (venv / container)            │
   │                                                          │
   │ prepare-run ─► run-agent ─► run-eval ─► summarize        │
   │      │             │            │           │            │
   │      ▼             ▼            ▼           ▼            │
   │ config.json   mini-swe-agent  SWE-bench   metrics.json   │
   │               (agent loop)    harness     manifest.json  │
   └───────┬───────────┬──────────────┬───────────┬───────┬───┘
           │           ▼              ▼           │       │
           │   ┌──────────────┐ ┌─────────────┐   │       │
           │   │ Nebius Token │ │Docker daemon│   │       │
           │   │ Factory (LLM)│ │(per-instance│   │       │
           │   └──────────────┘ │ eval ctrs)  │   │       │
           │                    └─────────────┘   ▼       ▼
           ▼                                 ┌────────┐ ┌────────┐
     runs/<run-id>/  ───────── upload ─────► │ MinIO  │ │ MLflow │
     (shared volume)                         │  (S3)  │ │ server │
                                             └────────┘ └────────┘
```

## 6. Core Mechanic — the four-task DAG

```
 Airflow params: split, subset, workers, model, task_slice, run_id, cost_limit
        │ (templated into CLI args; run_id defaults to a generated id)
        ▼
┌─────────────┐   run_id    ┌─────────────┐  preds.json  ┌─────────────┐  reports  ┌──────────────────┐
│ prepare_run │────────────►│  run_agent  │─────────────►│  run_eval   │──────────►│ summarize_and_log│
└─────────────┘             └─────────────┘              └─────────────┘           └──────────────────┘
 IN : params                 IN : config.json             IN : config.json,         IN : config.json,
 OUT: runs/<id>/config.json  OUT: run-agent/preds.json,        run-agent/preds.json      run-eval/reports/
      (full resolved config;      run-agent/trajectories/ OUT: run-eval/logs/,      OUT: metrics.json,
       the single source of       (one dir per instance)       run-eval/reports/         manifest.json,
       truth for later steps)                                  (harness summary          S3 upload,
                                                                + per-instance)          MLflow run
 retries: 0                  retries: 1                   retries: 1                 retries: 2
 timeout: 5m                 timeout: param-scaled        timeout: param-scaled      timeout: 5m
```

Inside `summarize_and_log` (CLI `summarize`), strictly ordered:
`collect metrics → write metrics.json → write manifest.json (incl. planned S3 URI) →
upload runs/<id>/ to S3 → log MLflow run (params + metrics + local path + S3 URI)`.
The S3 destination is computed *before* upload so the manifest inside the uploaded copy
already points at itself. Re-running the task is idempotent: S3 objects are overwritten,
and MLflow logging searches for an existing run tagged `run_id=<id>` before creating one.

"Param-scaled" timeouts above have concrete, evidence-based formulas — measured baselines
and worst-case math in [BREAKDOWN.md → Pipeline Runtime](BREAKDOWN.md): `run_agent` ≈
`ceil(n/workers) × 30 min`, `run_eval` ≈ `ceil(n/workers) × 10 min` + first-run pull allowance.

## 7. State Shape

**RunConfig — `runs/<run-id>/config.json`** (written once by `prepare-run`, read by every
later step; the reproducibility contract of SPEC 2.2):

```json
{
  "run_id": "20260702T142530__verified__0-3",
  "created_at": "2026-07-02T14:25:30Z",
  "split": "test",
  "subset": "verified",
  "workers": 4,
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:3",
  "cost_limit": 0.0,
  "dataset_name": "princeton-nlp/SWE-bench_Verified",
  "package_versions": {"mini-swe-agent": "2.4.1", "swebench": "4.1.0"}
}
```

**metrics.json** (parsed by `metrics.py` from the harness summary
`<model>.<run_id>.json` — see `sample/nebius__moonshotai__Kimi-K2.6.test.json`):

```json
{
  "total_instances": 500, "submitted_instances": 3, "completed_instances": 3,
  "resolved_instances": 1, "unresolved_instances": 2,
  "empty_patch_instances": 0, "error_instances": 0,
  "resolve_rate": 0.333
}
```

**manifest.json** — the "send this folder to a teammate" index:

```json
{
  "run_id": "...", "config": "config.json", "metrics": "metrics.json",
  "predictions": "run-agent/preds.json", "trajectories": "run-agent/trajectories/",
  "eval_logs": "run-eval/logs/", "eval_reports": "run-eval/reports/",
  "remote_artifact_uri": "s3://runs/20260702T142530__verified__0-3/",
  "mlflow": {"tracking_uri": "http://mlflow:5000", "experiment": "swe-bench-evals"}
}
```

## 8. Example Walkthroughs

**W1 — smoke run from the Airflow UI (easy mode).** User triggers `evaluate_agent` with
`split=test, subset=verified, workers=4, task_slice=0:3, cost_limit=0`.
`prepare_run` runs `uv run python -m pipeline.cli prepare-run --split test --subset verified
--workers 4 --task-slice 0:3 --cost-limit 0` → creates
`runs/20260702T142530__verified__0-3/config.json`, pushes `run_id` to XCom.
`run_agent` runs `... cli run-agent --run-dir runs/<id>` → `agent_runner.py` builds
`mini-extra swebench --subset verified --split test --model nebius/moonshotai/Kimi-K2.6
--slice 0:3 --workers 4 -o runs/<id>/run-agent/trajectories` (with
`MSWEA_COST_TRACKING=ignore_errors`). Note the output-shape mismatch: mini-swe-agent writes
instance dirs *and* `preds.json` into `-o`, but the SPEC 2.1 contract wants `preds.json` at
`run-agent/preds.json` beside `trajectories/` — so `run_agent()` moves it up one level after
the batch finishes. `run_eval` invokes
`python -m swebench.harness.run_evaluation --dataset_name princeton-nlp/SWE-bench_Verified
--predictions_path runs/<id>/run-agent/preds.json --max_workers 4 --run_id <id>`, then
relocates `logs/run_evaluation/...` and the summary JSON into `runs/<id>/run-eval/`.
`summarize_and_log` parses the summary → `metrics.json` (resolve_rate 0.333), writes
`manifest.json`, uploads the folder to `s3://runs/<id>/`, logs an MLflow run.

**W2 — reconstructing a run (SPEC 2.2 test).** A teammate downloads `s3://runs/<id>/`.
`manifest.json` indexes everything; `config.json` answers "which model / slice / split?";
`run-agent/trajectories/astropy__astropy-12907/*.traj.json` shows every agent step;
`run-eval/reports/` shows which tests passed; `metrics.json` has the headline numbers.
To rerun: trigger the DAG with the same params and `run_id=<id>-rerun` — nothing else needed.

**W3 — the same run under docker-compose (production mode).** `run_agent` is now a
`DockerOperator`: image `coding-agent-eval-harness:latest`, command
`python -m pipeline.cli run-agent --run-dir /mlops-assignment/runs/<id>` (the image's
`WORKDIR` is `/mlops-assignment`; `RUNS_ROOT` points there), mounts
`$HOST_PROJECT_DIR/runs → /mlops-assignment/runs` and `/var/run/docker.sock` (for `run_eval`'s
per-instance containers), network `coding-agent-eval-harness_default` so the container
resolves `http://mlflow:5000` and `http://minio:9000`. The CLI code is byte-identical to W1.

## 9. Persistence Architecture

```
                 hot / working                    durable / shared
        ┌────────────────────────────┐   ┌──────────────────────────────┐
        │ runs/<run-id>/  (volume)   │   │ MinIO  s3://runs/<run-id>/   │
        │  written by tasks as they  │──►│  full folder copy, uploaded  │
        │  execute; source of truth  │   │  once at summarize; URI in   │
        │  during the run            │   │  manifest + MLflow           │
        └────────────────────────────┘   ├──────────────────────────────┤
                                         │ MLflow (postgres/sqlite)     │
        Airflow metadata DB (postgres)   │  params, metrics, run_id,    │
        task state, retries, XCom(run_id)│  artifact URI — the queryable│
                                         │  cross-run comparison layer  │
                                         └──────────────────────────────┘
```

Three stores, three jobs: the **filesystem** is complete but local; **S3** is complete and
durable; **MLflow** is incomplete by design (numbers + pointers) but comparable across runs.

## 10. Component Organization

**`pipeline/config.py` [L0]** — `RunConfig` (frozen dataclass); `PARAM_DEFAULTS` (single
source for DAG Params and CLI); `resolve_config(cli_args) -> RunConfig` (fills defaults,
generates `run_id`); `RunConfig.from_json / to_json`. Stdlib only.

**`pipeline/artifacts.py` [L1]** — `RunPaths` (all paths derived from one root:
`config_path`, `agent_dir`, `preds_path`, `eval_dir`, `metrics_path`, `manifest_path`);
`init_run_dir(config) -> RunPaths`; `build_manifest(paths, remote_uri) -> dict`.

**`pipeline/agent_runner.py` [L2]** — `build_agent_command(config, paths) -> list[str]`
(pure — unit-testable without running anything); `run_agent(config, paths)` (subprocess +
env injection, then relocate `preds.json` from `trajectories/` up to `run-agent/preds.json`
and validate it is non-empty — see §8 W1).

**`pipeline/evaluator.py` [L2]** — `build_eval_command(config, paths) -> list[str]`;
`run_eval(config, paths)` (subprocess, then relocate harness outputs into `run-eval/`).

**`pipeline/metrics.py` [L2]** — `collect_metrics(paths) -> dict` (parse harness summary,
derive `resolve_rate`); `write_metrics(paths, metrics)`.

**`pipeline/storage.py` [L2]** — `make_s3_client(config)` (endpoint/creds from env — works
for MinIO and Nebius OS alike); `upload_run_dir(paths, bucket) -> str` (returns URI);
`planned_uri(run_id) -> str`.

**`pipeline/tracking.py` [L2]** — `log_run(config, metrics, artifact_uri)` (find-or-create
MLflow run tagged with `run_id`, log params/metrics/URI).

**`pipeline/cli.py` [L3]** — argparse with subcommands `prepare-run`, `run-agent`,
`run-eval`, `summarize`; each maps 1:1 to a DAG task; prints a one-line JSON result to
stdout (the DAG's subprocess wrapper can parse it if needed).

**`dags/evaluate_agent.py` [L4]** — `Param` declarations from `PARAM_DEFAULTS`; four tasks;
an executor switch: easy mode wraps commands in `subprocess.run(..., check=True)`, production
mode constructs `DockerOperator`s with the same command lists; retries/timeouts per §6.

## 11. CLI / API Surface

| Command | Inputs | Output / side effect |
|---|---|---|
| `python -m pipeline.cli prepare-run --split S --subset B --workers N [--model M --task-slice A:B --run-id ID --cost-limit C]` | params | creates `runs/<id>/config.json`; prints `{"run_id": ..., "run_dir": ...}` |
| `python -m pipeline.cli run-agent --run-dir DIR` | config.json, `NEBIUS_API_KEY` | `run-agent/preds.json` + `trajectories/` |
| `python -m pipeline.cli run-eval --run-dir DIR` | config.json, preds.json, Docker daemon | `run-eval/logs/` + `run-eval/reports/` |
| `python -m pipeline.cli summarize --run-dir DIR` | eval reports; `MLFLOW_TRACKING_URI`, S3 env | `metrics.json`, `manifest.json`, S3 upload, MLflow run; prints metrics |

Environment contract (full list in `.env.example`): `NEBIUS_API_KEY`,
`MLFLOW_TRACKING_URI`, `MLFLOW_EXPERIMENT_NAME`, `AWS_ENDPOINT_URL` (MinIO or Nebius OS),
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `RUNS_BUCKET`, `RUNS_ROOT` (defaults to
`./runs`; a container path under Docker), `HOST_PROJECT_DIR` (DockerOperator mounts).

## 12. Implementation Order

Mirrors the blocks in [CLAUDE.md](CLAUDE.md):

1. **A** — environment bring-up: `uv sync`, standalone Airflow, starter DAG runs (no new files)
2. **B** — `pipeline/config.py`, `pipeline/artifacts.py` (+ `__init__.py`)
3. **C** — `pipeline/agent_runner.py`, `pipeline/evaluator.py`, `pipeline/cli.py` (first 3 subcommands)
4. **D** — `pipeline/metrics.py`, `summarize` subcommand (metrics + manifest, no upload/MLflow yet)
5. **E** — `dags/evaluate_agent.py` (easy mode: params + subprocess + retries/timeouts) ← *speedrun complete*
6. **F** — `pipeline/tracking.py`; local `mlflow server`; wire into `summarize`
7. **G** — `pipeline/storage.py`; MinIO via `docker run`; wire into `summarize`
8. **H** — DockerOperator variant of the DAG; Dockerfile gains `COPY pipeline pipeline/`
9. **I** — `docker-compose.yaml` (Airflow LocalExecutor + postgres + MLflow + MinIO), env wiring
10. **J** — Nebius VM deployment; real batch; `screenshots/`
11. **K** — `REPORT.md`, final `README.md`, SPEC verification checklist sweep

## 13. Key Dependencies

| Dep | Version | Where it lives | Why |
|---|---|---|---|
| mini-swe-agent | ==2.4.1 (uv.lock) | project venv / task image | agent batch runner |
| swebench | ==4.1.0 (uv.lock) | project venv / task image | patch evaluation harness |
| mlflow | >=3.14,<4 | project venv / task image (client); compose (server) | tracking |
| boto3 | >=1.34 | project venv / task image | S3 upload |
| python-dotenv | >=1.0 | project venv | local `.env` loading in `config.py` |
| apache-airflow | 3.2.x | `uv tool` (standalone) / `apache/airflow:3.2.2` (compose) | orchestration |
| apache-airflow-providers-docker | >=4 | Airflow env (compose image or `_PIP_ADDITIONAL_REQUIREMENTS`) | DockerOperator |
| minio (server) | latest | compose | S3-compatible store |
| postgres | 16 | compose | Airflow metadata DB |

Pinned exactly in `pyproject.toml`/`uv.lock` (execution env) and `docker-compose.yaml`
(service images) when the corresponding blocks land.

## 14. Verification

Requirement-level checks: [SPEC.md → Verification Checklist](SPEC.md#verification-checklist).
Quick smoke tests per stage:

```bash
uv run python -m pipeline.cli prepare-run --split test --subset verified --workers 4 --task-slice 0:3 --cost-limit 0
cat runs/*/config.json                          # Block B/C sanity
uv run python -m pipeline.cli run-agent --run-dir runs/<id>   # 3 trajectories + preds.json
uv run python -m pipeline.cli summarize --run-dir runs/<id>   # metrics printed, MLflow run visible
bash run-airflow-standalone.sh                  # → trigger evaluate_agent in the UI (Block E)
docker compose up -d && docker compose ps       # all services healthy (Block I)
```
