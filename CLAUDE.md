# CLAUDE.md

## Project

An end-to-end MLOps pipeline that evaluates coding agents: Airflow orchestrates
mini-swe-agent over SWE-bench instances, evaluates the patches with the SWE-bench harness,
writes a reproducible `runs/<run-id>/` artifact tree, uploads it to S3 (MinIO), and logs
everything to MLflow. Requirements: [SPEC.md](SPEC.md). Architecture: [PLAN.md](PLAN.md).
Effort & concepts per block: [BREAKDOWN.md](BREAKDOWN.md). Original assignment:
`ASSIGNMENT.md` (gitignored).

### Tech Stack
- Python 3.12, uv (project env) — mini-swe-agent 2.4.1, swebench 4.1.0, mlflow, boto3
- Airflow 3.2.x (standalone via `uv tool` first, docker-compose later)
- MLflow 3.14.x, MinIO (S3-compatible), Docker + docker-compose
- Dev on macOS; real evaluation batches on a Nebius Linux VM (eval images are x86_64)

## My Learning Goals
- Model an ML experiment as a pipeline with explicit inputs, outputs, retries, dependencies
- Learn Airflow 3 properly: params, TaskFlow, XCom, operators, deployment
- Learn MLflow tracking: params/metrics/artifacts, comparing runs
- Practice production MLOps discipline: provenance, durability, reproducibility, isolation
- Understand how coding-agent evals work (trajectories, predictions, test-based judging)

## How to Work With Me

### Teaching Mode (default)
- **Do NOT write full implementations.** Guide step by step.
- **Explain the concept BEFORE writing code.**
- **Show small snippets** (≤20 lines), then let me extend them.
- **Hints before answers** when I'm stuck.
- **Ask leading questions** to check my understanding.
- **After each step, explain WHY** — connect to the architecture in PLAN.md.
- **Reference SPEC.md** to confirm we're meeting requirements (cite IDs like "2.3").

### When I say "just do it" / "implement this"
- Switch to implementation mode — write full code.
- Still explain non-obvious design decisions.
- Tick off the matching SPEC.md verification checkboxes.

### Code Standards
- Type hints on all function signatures; docstrings on public functions
- `build_*_command()` functions are pure (return `list[str]`, no side effects) — unit-testable
- Follow the layer rules in PLAN.md §2 — see Architecture Rules below
- No experiment values hard-coded outside `PARAM_DEFAULTS` (SPEC C1)
- Never commit secrets or runtime output (SPEC C2, C5)

## Implementation Steps

Work through these blocks in order. Each block is a self-contained conceptual unit that can
be reviewed and tested before moving on. Blocks A–E are the assignment's "speedrun"; F–I are
production polish; J–K are evidence and writeup.

---

### BLOCK A — Environment bring-up & starter tour (no new code)
**Concept:** what the ad-hoc world looks like before orchestration — and what Airflow standalone gives you for free.
**Outcome:** starter DAG runs end-to-end locally; you can read a trajectory and an eval report.

- [x] **A.1** `uv sync`, `cp .env.example .env`, add `NEBIUS_API_KEY` *(+ `uv python pin 3.12`)*
- [x] **A.2** Run `bash scripts/mini-swe-bench-single.sh`; inspect the produced `trajectory.json` *(94-step grind, no patch — saw an agent-level failure live)*
- [x] **A.3** Read `sample/`: a trajectory, `preds.json`, the harness summary JSON, a per-instance `report.json`
- [x] **A.4** `bash run-airflow-standalone.sh`, open http://localhost:8080, run the `mini-swe-bench-single` DAG *(run #2 overwrote run #1's trajectory.json — hard-coded `-o` = no run isolation)*
- [x] **A.5** Explain back: why does the starter DAG shell out via `uv run` instead of importing mini-swe-agent?
- **Learn:** mini-swe-agent CLI (`mini-extra swebench[-single]`), SWE-bench output anatomy, Airflow standalone, the isolated-envs problem that drives PLAN §1

---

### BLOCK B — Run config & artifact contract (Layers 0–1)
**Concept:** configuration-as-data and the run directory as a reproducibility contract.
**Outcome:** `pipeline/config.py` + `pipeline/artifacts.py`; a run dir with `config.json` can be created from Python.

- [x] **B.1** `pipeline/config.py`: `RunConfig` frozen dataclass + `PARAM_DEFAULTS` (stdlib only!)
- [x] **B.2** `resolve_config()`: fill defaults, generate `run_id` (`<timestamp>__<subset>__<slice>`)
- [x] **B.3** `to_json`/`from_json` round-trip; record package versions (SPEC 2.2)
- [x] **B.4** `pipeline/artifacts.py`: `RunPaths` — every path derived from one root (PLAN §10)
- [x] **B.5** `init_run_dir()`; verify the tree matches SPEC 2.1 exactly *(17 unit tests in tests/)*
- **Learn:** frozen dataclasses, single-source-of-truth config, why `run_id` generation must be deterministic and collision-safe

---

### BLOCK C — Agent & eval wrappers + CLI (Layers 2–3)
**Concept:** wrapping ad-hoc scripts as code, separating *what to run* (pure command builders) from *how to run it* (subprocess now, Docker later).
**Outcome:** `python -m pipeline.cli prepare-run|run-agent|run-eval` produce a populated run dir.

- [x] **C.1** `agent_runner.py`: `build_agent_command()` translating RunConfig → `mini-extra swebench` args (mirror `scripts/mini-swe-bench-batch.sh`)
- [x] **C.2** `run_agent()`: subprocess + `MSWEA_COST_TRACKING=ignore_errors` env; relocate `preds.json` out of `trajectories/` to match SPEC 2.1 (PLAN §8 W1), validate non-empty
- [x] **C.3** `evaluator.py`: `build_eval_command()` → `swebench.harness.run_evaluation` args; `run_eval()` relocates logs/summary into `run-eval/`
- [x] **C.4** `cli.py`: argparse subcommands `prepare-run`, `run-agent`, `run-eval`; one-line JSON to stdout
- [x] **C.5** Smoke test: 1-instance live run via CLI — astropy-12907 submitted+resolved (resolve_rate 1.0); workers-interleaving check deferred to the Block E multi-instance trigger
- **Learn:** subprocess.run patterns (check, env, cwd), pure command builders, where the harness writes its outputs and why we relocate them

---

### BLOCK D — Metrics (Layer 2)
**Concept:** anatomy of SWE-bench evaluation output; turning reports into comparable numbers.
**Outcome:** `summarize` subcommand writes `metrics.json` + `manifest.json` (no MLflow/S3 yet).

- [x] **D.1** `metrics.py`: parse the harness summary JSON (see `sample/nebius__moonshotai__Kimi-K2.6.test.json`), derive `resolve_rate`
- [x] **D.2** `artifacts.py`: `build_manifest()` per PLAN §7 (remote URI empty for now)
- [x] **D.3** `summarize` subcommand wiring metrics + manifest
- [x] **D.4** Pass the SPEC 2.2 test: reconstruct your smoke run from the folder alone
- **Learn:** resolved vs completed vs submitted, FAIL_TO_PASS / PASS_TO_PASS semantics, manifest-as-index pattern

---

### BLOCK E — The configurable DAG (Layer 4) 🏁 *speedrun complete*
**Concept:** Airflow 3 params, TaskFlow, XCom, retries/timeouts — one button runs the whole pipeline.
**Outcome:** `dags/evaluate_agent.py`; a UI-triggered 3-instance run produces a complete run dir. SPEC Area 1 done.

- [x] **E.1** `Param` declarations built from `PARAM_DEFAULTS` (import Layer 0 only — PLAN §2!)
- [x] **E.2** `prepare_run` task: subprocess the CLI, parse `run_id` from stdout, push via XCom *(gotcha: `run_id` is a reserved TaskFlow kwarg → `pipeline_run_id`)*
- [x] **E.3** `run_agent` / `run_eval` / `summarize_and_log` tasks consuming the XCom `run_id`
- [x] **E.4** Retries + timeouts per PLAN §6 (SPEC C3) *(a real zombie task got auto-retried and the retry resumed the batch — policy verified live)*
- [x] **E.5** Trigger twice with different params → two independent run dirs (SPEC 1.2.5) *(0:2/w2 + 2:3/w1, both success, workers interleaving confirmed)*
- [x] **E.6** Grep the DAG for hard-coded experiment values (SPEC 1.1.3) *(clean)*
- **Learn:** `@dag`/`@task`, `Param` + trigger form, XCom mechanics, retry/timeout semantics, why the DAG imports almost nothing

---

### BLOCK F — MLflow tracking (Layer 2)
**Concept:** experiment tracking — params/metrics/artifact-refs as the queryable comparison layer.
**Outcome:** every pipeline run appears in MLflow; two runs comparable side by side. SPEC Area 3 done.

- [x] **F.1** Run a local server: `uv run mlflow server --port 5000 --backend-store-uri sqlite:///mlflow.db` *(macOS: browse via 127.0.0.1 — AirPlay owns localhost:5000)*
- [x] **F.2** `tracking.py`: `log_run()` — find-or-create by `run_id` tag (idempotent retries, PLAN §6; verified same mlflow_run_id on re-run)
- [x] **F.3** Wire into `summarize`; env contract `MLFLOW_TRACKING_URI`, `MLFLOW_EXPERIMENT_NAME`
- [x] **F.4** Three runs logged (slices 0:1, 0:2, 2:3; rates 1.0/0.5/1.0) → comparable in the UI (SPEC 3.4)
- **Learn:** MLflow experiments/runs/tags, params vs metrics vs artifacts, logging references instead of copies, idempotent logging

---

### BLOCK G — Object storage upload (Layer 2)
**Concept:** durable artifacts via the S3 API; MinIO locally, Nebius Object Storage in prod — same boto3 code.
**Outcome:** run folders land in `s3://runs/<run-id>/`; URI in manifest + MLflow. SPEC 2.4 done.

- [ ] **G.1** MinIO via `docker run` (ports 9000/9001); create the `runs` bucket
- [ ] **G.2** `storage.py`: client from env (`AWS_ENDPOINT_URL` — the swappability trick), `upload_run_dir()`
- [ ] **G.3** Order-of-operations in `summarize`: manifest gets the *planned* URI before upload (PLAN §6)
- [ ] **G.4** Downloaded the folder from MinIO (`download_run_dir()`); SPEC 2.2 reconstruction passed on the copy
- **Learn:** S3 object model (no real directories), boto3 upload patterns, endpoint-based portability, why manifest-before-upload

---

### BLOCK H — Execution isolation with DockerOperator (Layer 4)
**Concept:** same CLI, different executor — containers as reproducible task environments.
**Outcome:** `run_agent`/`run_eval` run in the project image from standalone Airflow. SPEC Area 4 done.

- [ ] **H.1** Extend `Dockerfile`: `COPY pipeline pipeline/` (+ `.python-version`); build `coding-agent-eval-harness:latest` (2 GB)
- [ ] **H.2** Docker provider: `--with apache-airflow-providers-docker` in standalone script; built into the compose image
- [ ] **H.3** `EXECUTION_MODE=docker` switch: `run_agent`/`run_eval`/`summarize_and_log` become DockerOperators (same CLI commands; env passthrough via `TASK_ENV_KEYS`)
- [ ] **H.4** Mount `runs/` (host path!) and `/var/run/docker.sock`; sibling eval containers observed live
- [ ] **H.5** Full run through Docker tasks (combined with I.5); identical run-dir shape confirmed
- **Learn:** DockerOperator (image/command/mounts/env), docker-out-of-docker vs docker-in-docker, host-path vs container-path for bind mounts

---

### BLOCK I — docker-compose deployment (infra)
**Concept:** composing Airflow + MLflow + MinIO into one deployable stack.
**Outcome:** `docker compose up` brings up everything; the DAG reaches services by name. SPEC Area 5 done.

- [ ] **I.1** Start from the official Airflow 3.2 compose file; strip to LocalExecutor + postgres
- [ ] **I.2** Add `mlflow` and `minio` services (+ bucket-init job); volumes for durability
- [ ] **I.3** Env wiring: extend `.env.example` (SPEC 5.2); `HOST_PROJECT_DIR` for DockerOperator mounts
- [ ] **I.4** Attach DockerOperator containers to the compose network
- [ ] **I.5** Full smoke run under compose (trigger → 4 tasks → resolved 1/1 → MLflow + MinIO in-network)
- **Learn:** compose services/networks/volumes/healthchecks, Airflow-in-Docker layout, the DockerOperator-under-compose host-path gotcha (PLAN §8 W3)

---

### BLOCK J — VM deployment & the real evaluation run (ops)
**Concept:** shipping to the target environment and producing graded evidence.
**Outcome:** completed real batch on the Nebius VM; three screenshots. SPEC 6.2–6.3 done.

- [ ] **J.1** Provision VM per ASSIGNMENT.md prereqs (8 CPU / 32 GB, Docker, uv); clone repo, `.env`
- [ ] **J.2** `docker compose up -d`; SSH port-forward 8080 (Airflow) + 5000 (MLflow) + 9001 (MinIO)
- [ ] **J.3** Trigger a real batch (e.g. `task_slice 0:10`, `workers 4+`); watch task logs
- [ ] **J.4** Capture `screenshots/airflow_dag.png`, `mlflow_runs.png`, `object_storage_artifacts.png`
- [ ] **J.5** Commit one sample `runs/<run-id>/` manifest (or trimmed folder) as evidence
- **Learn:** remote Docker ops, SSH tunneling, x86-only eval images (why the VM matters), sizing workers vs CPU

---

### BLOCK K — REPORT.md, README & final verification (polish)
**Concept:** the writeup is a graded artifact (10%) — reproducibility in prose.
**Outcome:** REPORT.md + portfolio README complete; every SPEC checkbox ticked.

- [ ] **K.1** `REPORT.md`: architecture (reuse PLAN diagrams), trigger instructions, artifact layout, MLflow evidence, one completed run analysis, rerun-by-run-id *(drafted; finalize with J evidence)*
- [ ] **K.2** Fill in `README.md` skeleton: demo screenshot, quick start, architecture summary *(quick start + real example done; demo screenshot after J)*
- [ ] **K.3** Sweep SPEC.md Verification Checklist — every box, honestly
- [ ] **K.4** Repo hygiene: no secrets, no stray outputs, no homework language (then consider `/dehomework-repo`)
- **Learn:** writing for reproducibility, portfolio framing of course work

---

## Architecture Rules (from PLAN.md — enforceable in chat)

1. Imports point **down** the layers only: config ← artifacts ← services ← cli ← DAG.
2. The DAG imports **only `pipeline/config.py`** (stdlib-only); it reaches everything else via
   the CLI as subprocess/DockerOperator commands.
3. Heavy deps (mlflow, boto3, swebench) are imported only in Layer 2 modules.
4. Services never import each other — `cli.py` orchestrates.
5. `build_*_command()` stays pure; side effects live in `run_*()`.
6. Experiment values exist only in `PARAM_DEFAULTS` and Airflow trigger forms.

## Quick Commands

```bash
# Install & env
uv sync && cp .env.example .env      # then add NEBIUS_API_KEY

# Easy-mode Airflow (Blocks A–H)
bash run-airflow-standalone.sh       # UI: http://localhost:8080 (admin/admin)

# Pipeline CLI directly (Blocks C–G)
uv run python -m pipeline.cli prepare-run --split test --subset verified --workers 4 --task-slice 0:3 --cost-limit 0
uv run python -m pipeline.cli run-agent  --run-dir runs/<id>
uv run python -m pipeline.cli run-eval   --run-dir runs/<id>
uv run python -m pipeline.cli summarize  --run-dir runs/<id>

# Services (Blocks F–I)
uv run mlflow server --port 5000     # until compose exists
docker compose up -d                 # Block I onward: Airflow + MLflow + MinIO
```
