# REPORT — Coding-Agent Evaluation Pipeline

An end-to-end MLOps pipeline that evaluates coding agents: a parameterized Airflow DAG runs
[mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) over SWE-bench instances,
judges the patches with the SWE-bench harness, writes a reproducible `runs/<run-id>/`
artifact tree, uploads it to S3-compatible object storage (MinIO), and registers every run
in MLflow for side-by-side comparison.

Requirements: [SPEC.md](SPEC.md) · Architecture: [PLAN.md](PLAN.md)

---

## 1. Architecture

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

**The load-bearing decision:** orchestration and execution live in different Python
environments, connected only by a CLI contract (`python -m pipeline.cli <step>`). The DAG
file imports a single stdlib-only module (`pipeline/config.py`) and shells everything else
out. Consequences:

- Airflow's env needs zero project dependencies (no mlflow/boto3/swebench at parse time).
- The same DAG runs in two executor modes — `EXECUTION_MODE=subprocess` (dev) and
  `EXECUTION_MODE=docker` (DockerOperator + pinned image) — because only the *executor*
  changes, never the command.
- Per-instance SWE-bench evaluation containers are started through the mounted Docker
  socket, making them *siblings* of the task container (docker-out-of-docker).

The four tasks, with retry/timeout policy:

| task | does | retries | timeout |
|---|---|---|---|
| `prepare_run` | resolve params → `runs/<run-id>/config.json`, push run_id to XCom | 0 | 1 m |
| `run_agent` | mini-swe-agent batch → trajectories + `preds.json` | 1 | 120 m (env-tunable) |
| `run_eval` | SWE-bench harness → per-instance verdicts | 1 | 90 m (env-tunable) |
| `summarize_and_log` | metrics → manifest → S3 upload → MLflow (idempotent) | 2 | 5 m |

`summarize_and_log` is strictly ordered so the manifest *inside the uploaded copy* already
carries its own S3 URI (destination computed before upload), and retries are safe: S3
overwrites, MLflow finds-or-creates by `run_id` tag.

## 2. How to trigger a run

**UI:** open Airflow (8080) → `evaluate_agent` → Trigger → the form (generated from
`PARAM_DEFAULTS`) offers `split, subset, model, task_slice, workers, cost_limit, run_id`.
Every experiment value is a parameter — nothing is hard-coded in the DAG (SPEC C1).

**CLI equivalent (same code path the DAG uses):**

```bash
uv run python -m pipeline.cli prepare-run --split test --subset verified --workers 4 --task-slice 0:3
uv run python -m pipeline.cli run-agent  --run-dir runs/<id>
uv run python -m pipeline.cli run-eval   --run-dir runs/<id>
uv run python -m pipeline.cli summarize  --run-dir runs/<id>
```

**Rerun by run id:** trigger the DAG with identical params plus `run_id=<id>-rerun`.
`prepare-run` fails loudly (`FileExistsError`) rather than overwrite an existing run dir.

## 3. Artifact layout (the reproducibility contract)

```
runs/<run-id>/                       # <run-id> = <timestamp>__<subset>__<slice>
├── config.json                      # frozen RunConfig incl. package versions
├── run-agent/
│   ├── preds.json                   # instance_id → model_patch (the deliverable)
│   └── trajectories/<iid>/*.traj.json   # every agent step (the evidence)
├── run-eval/
│   ├── logs/<iid>/                  # patch.diff, eval.sh, test_output.txt, report.json
│   └── reports/                     # summary.json + per-instance verdicts
├── metrics.json                     # counters + resolve_rate
└── manifest.json                    # index of all of the above + S3 URI + MLflow pointers
```

Verified reconstruction test (SPEC 2.2/2.4): the folder was downloaded back from
`s3://runs/<run-id>/` into a clean location and answered, from files alone: which model
(`nebius/moonshotai/Kimi-K2.6`), which slice (`0:1` of verified/test), the resolve rate
(1.0), and where full artifacts live (the manifest's own S3 URI).

## 4. Completed evaluation (analysis of one real run)

Run `20260707T214048__verified__0-1` (smoke run, 1 instance, executed live):

- **Agent phase:** `astropy__astropy-12907` — the agent read the issue, localized
  `astropy/modeling/separable.py`, produced a 504-char patch, and submitted after 24 LLM
  calls (exit status `Submitted`).
- **Eval phase:** the harness applied the patch in a fresh x86_64 instance container;
  both `FAIL_TO_PASS` tests flipped green and all 13 `PASS_TO_PASS` regression tests held
  → `"resolved": true`.
- **Metrics:** `submitted 1 · completed 1 · resolved 1 · resolve_rate 1.0` — logged to
  MLflow with the full parameter set, the local run path, and the S3 URI.

A second DAG-triggered run (`task_slice 2:3`) resolved `astropy__astropy-13236` — an
instance the reference sample had *unresolved*, a reminder that single-instance agent runs
have high variance; resolve rates only stabilize over larger slices.

## 5. MLflow evidence

Every pipeline run appears in experiment `swe-bench-evals`, tagged `run_id=<run-id>`,
with params (split/subset/model/slice/workers/cost_limit + package versions), metrics
(submitted/completed/resolved/unresolved/error counts + resolve_rate), and artifact
references (local path + S3 URI as tags). Runs are compared side by side in the UI via
checkbox → Compare.

<!-- TODO(Block J): screenshots/mlflow_runs.png + airflow_dag.png + object_storage_artifacts.png -->
*Screenshots from the VM deployment land in `screenshots/` (Block J).*

## 6. Deployment modes

| | easy mode | production mode |
|---|---|---|
| bring-up | `bash run-airflow-standalone.sh` | `docker compose up -d` |
| Airflow | standalone, SQLite | apiserver/scheduler/dag-processor/triggerer + postgres (LocalExecutor) |
| execution | subprocess into project venv | `DockerOperator` on `coding-agent-eval-harness:latest` |
| MLflow / MinIO | run manually when needed | compose services (`http://mlflow:5000`, `http://minio:9000`) |
| env | `.env` + host paths | `.env` + compose overrides (in-network endpoints) |

Same DAG file, same CLI, same artifact tree in both.

## 7. Operational notes & gotchas (learned the hard way)

- **macOS AirPlay squats on port 5000**: use `http://127.0.0.1:5000` (or remap
  `MLFLOW_PORT`) — `localhost` resolves to `::1` where AirPlay answers 403.
- **MLflow 3.x rejects unknown Host headers** (DNS-rebinding protection, also a 403 —
  a *different* 403 on the same port). In-network clients arrive as `mlflow:5000`, so the
  compose service sets `MLFLOW_SERVER_ALLOWED_HOSTS=mlflow:5000,...`.
- **`run_id` is a reserved TaskFlow parameter name** — task functions use
  `pipeline_run_id`.
- **mini-swe-agent writes `preds.json` inside its output dir** — `run_agent()` relocates
  it to `run-agent/preds.json` to keep the SPEC 2.1 shape.
- **The harness writes relative to cwd** — `run_eval()` runs it with `cwd=run-eval/` and
  reshapes `logs/run_evaluation/<rid>/<model>/<iid>/` → `logs/<iid>/`.
- **cost_limit is a no-op for Kimi-via-Nebius** (litellm has no pricing entry → tracked
  cost stays $0; `MSWEA_COST_TRACKING=ignore_errors` required). The real bound is the
  agent's step limit.
- **Zombie tasks get retried correctly**: a run_agent supervisor died mid-batch during
  development; Airflow's heartbeat detection marked it `UP_FOR_RETRY`, and the retry
  skipped already-completed instances (mini-swe-agent resumes) — the retry policy is not
  decorative.
- **libraries print to stdout** (mlflow's "View run" banner) — the CLI redirects all
  in-process stdout to stderr and emits exactly one JSON line on the real stdout, because
  the DAG parses it.

## 8. Verification status

See the ticked checklist in [SPEC.md](SPEC.md) (Verification Checklist). Unit tests cover
the pure layers (`uv run pytest` — config, artifacts, command builders, metrics, manifest,
storage key layout, MLflow idempotency).
