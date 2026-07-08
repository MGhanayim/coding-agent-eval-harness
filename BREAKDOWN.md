# BREAKDOWN.md — Effort & Knowledge by Block

> Organized by the conceptual blocks in [CLAUDE.md](CLAUDE.md). Each block maps to grading
> areas in [SPEC.md](SPEC.md) — this assignment grades by **percentage weights**, not points.

## Block → Assignment Mapping

> Simplified one-to-one view. See the many-to-many table below for the honest picture.

| Block | Theme | Maps to SPEC.md | Weight |
|-------|-------|-----------------|-------:|
| A | Environment bring-up & starter tour | prerequisites (ASSIGNMENT.md) | — |
| B | Run config & artifact contract | Area 2 (2.1–2.2) | 20%* |
| C | Agent & eval wrappers + CLI | Areas 1–2 plumbing | * |
| D | Metrics | Area 2 (2.2–2.3) | * |
| E | The configurable DAG 🏁 | **Area 1** | **35%** |
| F | MLflow tracking | **Area 3** | **15%** |
| G | Object storage upload | Area 2 extra credit (2.4) | +EC |
| H | DockerOperator isolation | **Area 4** | **10%** |
| I | docker-compose deployment | **Area 5** | **10%** |
| J | VM deployment & real run | Area 6 evidence (6.2–6.3) | * |
| K | REPORT.md & final verification | **Area 6** | **10%** |

\* shared — see below.

### Grading Line-Item → Blocks (the honest many-to-many view)

The rubric has 6 line-items, but each is a *capability*, not a file. Each is built across
several blocks:

| Grading line-item | Weight | Built across blocks |
|-------------------|-------:|---------------------|
| Configurable Airflow DAG | 35% | **E** (the DAG itself) + B (param defaults, no hard-codes) + C (the commands the tasks run) |
| Artifact structure & reproducibility | 20% | **B** (tree contract) + **C** (outputs land in it) + **D** (metrics/manifest) + G (S3 = extra credit within this area) |
| MLflow tracking | 15% | **F** (client + logging) + D (the metrics being logged) + I (server deployment) |
| Execution isolation | 10% | **H** (DockerOperator) + A (provided Dockerfile understood) + C (CLI design is what makes containerization a no-op) |
| Docker Compose deployment | 10% | **I** entirely |
| Report & reproducibility | 10% | **K** (writeup) + **J** (evidence: real run + screenshots) + all planning docs feeding it |

**Key implications:**
- Block C is load-bearing for **three** areas (1, 2, 4) despite "belonging" to none — the
  CLI-as-contract design is why DockerOperator later costs hours, not days.
- Area 2 (20%) has no single "artifact block": it's B+C+D+G. Skipping D breaks it.
- Blocks A and J earn no direct weight but gate everything: A unblocks development,
  J produces the evidence Area 6 is graded on.

## Effort Estimation by Block

Manual = knows Python well, learning Airflow/MLflow/Docker-compose as they go.
With AI = same person in teaching mode with Claude; bottleneck shifts to reviewing,
debugging, and *wall-clock* waits (installs, image builds, eval runs) that AI can't compress.

| Block | What gets built | Manual | With AI | Cloud VM | Complexity |
|-------|-----------------|-------:|--------:|:--------:|------------|
| A | working env, starter DAG run, format tour | 3h | 2h | optional° | Low (mostly wall-clock) |
| B | config.py, artifacts.py | 3h | 1h | — | Low |
| C | agent_runner, evaluator, cli + smoke run | 5h | 2h | optional° | Medium (output relocation is fiddly) |
| D | metrics.py, manifest, summarize | 2h | 0.5h | — | Low |
| E | evaluate_agent.py DAG, params, retries | 5h | 1.5h | optional° | Medium (Airflow 3 params/XCom learning curve) |
| F | tracking.py + local MLflow server | 3h | 1h | — | Low-Medium |
| G | storage.py + MinIO | 3h | 1h | — | Low-Medium |
| H | DockerOperator tasks + image | 5h | 2h | optional° | **High** (socket mount, host paths, env passthrough) |
| I | docker-compose.yaml, service wiring | 6h | 2.5h | optional° | **High** (Airflow compose + network gotchas) |
| J | VM deploy, real batch, screenshots | 4h | 3h | **required** | Medium (mostly wall-clock + ops) |
| K | REPORT.md, README, SPEC sweep | 3h | 1h | — | Low |

° These blocks end with a smoke run that executes x86_64 SWE-bench containers (agent and/or
eval). On Apple Silicon they work under emulation — slower and occasionally flaky — so running
just the smoke test on the VM over SSH is a comfortable alternative. The code itself is
developed locally either way. Only **Block J** genuinely needs the VM: it *is* the deployment
target, the graded run, and the screenshots.

### Totals

| Scope | Blocks | Manual | With AI |
|-------|--------|-------:|--------:|
| Speedrun (minimum submission) | A–E | 18h | 7h |
| + Production polish | F–I | +17h | +6.5h |
| + Evidence & writeup | J–K | +7h | +4h |
| **Everything (chosen scope)** | A–K | **~42h** | **~17.5h** |

Ratio ≈ 2.4× — lower than typical (3–4×) because this project is infra-heavy: image builds,
compose bring-up, and real eval batches take the same wall-clock time either way.

## Pipeline Runtime on the Cloud (Nebius VM, 8 vCPU / 32 GB)

> Effort above = *your* hours. This section = *pipeline* wall-clock. Measured from the
> timestamped logs in `sample/` (the assignment author's real Kimi-K2.6 run) plus official
> SWE-bench harness figures — not guesses.

### Measured baselines (from `sample/`)

| Phase | Measured | Notes |
|---|---|---|
| Agent, per instance | 1.7–3.6 min (avg ~2.7) | 16/26/71 steps; LLM-bound, not CPU-bound (Nebius serves K2.6 at ~198 tok/s) |
| Agent, 3-instance batch | 8.0 min | ran **sequentially** despite `--workers 5` — see gotchas |
| Eval, per instance | ~1.3 min | ~62–66 s pip/C-extension rebuild + fixed 15 s docker stop-grace; the tests themselves take <3 s |
| Eval, 3-instance batch | ~82 s wall | all three instances in parallel |

### Scenario estimates (workers 4–6)

| Scenario | First run | Warm (images cached) |
|---|---|---|
| Smoke `task_slice 0:3` | ~10–15 min | ~5–8 min |
| Graded batch `0:10` (Block J) | ~30–45 min | ~15–25 min |
| `0:50` | ~1.5–2.5 h | ~45–90 min |
| Full Verified (500) | ~6–9 h | ~4–6 h |

First-run premium = pulling prebuilt x86_64 instance images from Docker Hub
(~0.4 GiB/instance; ~189 GiB for all 500). The pull hits during the first **run_agent** —
mini-swe-agent's Docker environment uses the same instance images as the eval harness.
The 500-row eval side derives from the only official wall-clock table (SWE-bench *Lite*,
300 instances ≈ 50 min on 8-core/6-worker with env cache), scaled to Verified.

### Planning implications & gotchas

- **Airflow timeouts (Block E.4):** `run_agent` ≈ `ceil(n/workers) × 30 min` (step-limit
  worst case); `run_eval` ≈ `ceil(n/workers) × 10 min` + 30 min first-run pull allowance.
- **`cost_limit` is a no-op with Nebius K2.6:** `MSWEA_COST_TRACKING=ignore_errors` +
  K2.6 missing from litellm's price registry → `instance_cost` stays 0.0 and the limit never
  triggers. The real per-instance bound is `step_limit 250 × (LLM latency + ≤60 s command
  timeout)` ≈ 20–30 min worst case.
- **Verify `--workers` actually parallelizes** (Block C.5): the sample batch silently ran
  sequential — look for *interleaved* "Starting container" lines in the agent log. Sequential
  execution triples the Block J wall-clock.
- **Disk:** ~10 GB covers assignment-scale slices; 120 GB free is the official recommendation
  for big sweeps.
- **Sample bias:** all three sample instances are astropy — django/sympy instances have wider
  install/test-time spread; the ranges above pad for it.
- **Block J budget:** one ~2–3 h VM session covers compose bring-up + two graded runs +
  screenshots.

Sources: measured — `sample/trajectories/minisweagent.log`,
`sample/logs/run_evaluation/.../run_instance.log`; published —
[SWE-bench harness reference](https://www.swebench.com/SWE-bench/reference/harness/),
[SWE-bench docker guide](https://www.swebench.com/SWE-bench/guides/docker_setup/),
[Epoch AI on SWE-bench images](https://epoch.ai/blog/swebench-docker),
[Artificial Analysis — K2.6 on Nebius](https://artificialanalysis.ai/models/kimi-k2-6/providers).

## Knowledge by Block

### Block A — Environment bring-up & starter tour
**AI/ML Concepts & Architecture:**
- **Agentic coding evaluation** — an agent gets a real GitHub issue in a sandbox, emits a patch, and real unit tests judge it (FAIL_TO_PASS must flip, PASS_TO_PASS must hold)
- **Harness vs model** — the two levers of agent quality the pipeline exists to compare
- **Trajectory** — the full step-by-step record of an agent run; the unit of debuggability

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `mini-extra swebench[-single]` flags: `--subset --split --model --slice --workers -o` | mini-swe-agent 2.4.1 |
| `run_evaluation` flags: `--dataset_name --predictions_path --max_workers --run_id` | swebench 4.1.0 |
| `standalone` mode, DAGS_FOLDER, simple auth | Airflow 3.2 |
| `uv sync` vs `uv run` vs `uv tool run` (three different envs!) | uv |

### Block B — Run config & artifact contract
**AI/ML Concepts & Architecture:**
- **Config-as-data** — one frozen RunConfig written to disk beats scattered variables; it *is* the experiment's identity
- **Reproducibility contract** — a run dir a stranger can fully interpret (SPEC 2.2)

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `@dataclass(frozen=True)`, `asdict`, JSON round-trip | dataclasses / json |
| `Path` composition, `mkdir(parents=True)` | pathlib |
| `importlib.metadata.version()` for recording package versions | stdlib |

### Block C — Agent & eval wrappers + CLI
**AI/ML Concepts & Architecture:**
- **Command-builder purity** — separating *what to run* from *how to run it*; the design that later makes DockerOperator a drop-in
- **Wrapping ad-hoc scripts** — parameterize, validate outputs, relocate into the contract

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `subprocess.run(check=True, env=..., cwd=...)` | subprocess |
| subcommands via `add_subparsers` | argparse |
| where the harness writes `logs/run_evaluation/...` + summary JSON | swebench |
| `MSWEA_COST_TRACKING=ignore_errors` env quirk | mini-swe-agent |

### Block D — Metrics
**AI/ML Concepts & Architecture:**
- **Eval-output anatomy** — submitted vs completed vs resolved vs empty-patch vs error; `resolve_rate` as the headline metric
- **Manifest-as-index** — small JSON pointing at big files; ship the pointer, not the data

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| harness summary schema (see `sample/nebius__moonshotai__Kimi-K2.6.test.json`) | swebench |
| per-instance `report.json`: FAIL_TO_PASS / PASS_TO_PASS | swebench |

### Block E — The configurable DAG
**AI/ML Concepts & Architecture:**
- **Pipeline-as-DAG** — explicit dependencies, retries, timeouts instead of manual shell ordering
- **Parameterized experiments** — the trigger form as the experiment interface (no code edits per run)

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `@dag` / `@task` decorators (TaskFlow) | Airflow 3.2 |
| `Param(default, type, description)` + `params` in context | Airflow |
| XCom push/pull between tasks | Airflow |
| `retries`, `retry_delay`, `execution_timeout` | Airflow |

### Block F — MLflow tracking
**AI/ML Concepts & Architecture:**
- **Experiment tracking** — params/metrics/tags as the queryable layer over runs; compare across time, models, configs
- **References over copies** — log the artifact URI, not gigabytes of trajectories
- **Idempotent logging** — retried task must not create duplicate runs (find-or-create by tag)

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `mlflow server`, `set_tracking_uri`, `set_experiment` | MLflow 3.14 |
| `start_run`, `log_params`, `log_metrics`, `set_tag`, `search_runs` | MLflow |

### Block G — Object storage upload
**AI/ML Concepts & Architecture:**
- **Artifact durability** — the VM is disposable; S3 is the archive of record
- **S3-API portability** — MinIO locally, Nebius Object Storage in prod; only `AWS_ENDPOINT_URL` changes

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `boto3.client("s3", endpoint_url=...)`, `upload_file`, keys not dirs | boto3 |
| server + console, `mc` or console bucket creation | MinIO |

### Block H — DockerOperator isolation
**AI/ML Concepts & Architecture:**
- **Execution isolation** — pinned-image containers as the reproducible task environment (the production-style path; K8sPodOperator is the same idea at scale)
- **Docker-out-of-docker** — mounting `/var/run/docker.sock` makes eval containers *siblings*; bind-mount paths must be **host** paths

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `DockerOperator(image, command, mounts, environment, network_mode)` | apache-airflow-providers-docker |
| `Mount(source, target, type="bind")` | docker SDK |
| multi-stage-ish uv image, `uv sync --locked` | Dockerfile (provided) |

### Block I — docker-compose deployment
**AI/ML Concepts & Architecture:**
- **Deployment-as-code** — the whole tracking+orchestration stack from one file
- **Service discovery** — containers reach `http://mlflow:5000` by service name on the compose network

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| official Airflow 3.2 compose file; trimming Celery→LocalExecutor | docker compose |
| services, volumes, healthchecks, `depends_on`, networks | docker compose |
| `_PIP_ADDITIONAL_REQUIREMENTS` vs extending the Airflow image | Airflow docker |
| backend store vs artifact destination flags | MLflow server |

### Block J — VM deployment & real run
**AI/ML Concepts & Architecture:**
- **Target-environment reality** — SWE-bench eval images are x86_64; Apple Silicon emulation is why the real batch runs on the VM
- **Capacity sizing** — `workers` vs 8 CPUs vs per-instance container overhead

**Libraries & Tools:**
| What you need to know | Library / Tool |
|---|---|
| `ssh -L` port-forwarding (8080/5000/9001) | ssh |
| remote compose ops, `docker compose logs -f` | docker |

### Block K — REPORT.md & final verification
**AI/ML Concepts & Architecture:**
- **Provenance writing** — the grader's stated bar: a reproducible weak result beats an unreproducible good one
- **Portfolio framing** — same repo, two audiences (grader, hiring manager)

**Libraries & Tools:** none new — this block spends the knowledge of all previous ones.

## Dataset / API Quick Reference

| Thing | Value |
|---|---|
| Dataset | `princeton-nlp/SWE-bench_Verified` (HuggingFace) — 500 human-validated instances |
| Selection | `--subset verified --split test --slice A:B` (slice = cheap smoke runs) |
| LLM API | Nebius Token Factory, OpenAI-compatible; model id `nebius/moonshotai/Kimi-K2.6`; auth via `NEBIUS_API_KEY` |
| Agent output | `preds.json` (`{instance_id: {model_name_or_path, instance_id, model_patch}}`) + per-instance trajectory dirs |
| Eval output | `logs/run_evaluation/<run_id>/<model>/<instance>/` + summary `<model>.<run_id>.json` |
| Sample of both | `sample/` in this repo |

## Model / Tech Choices

| Choice | Decision | Why |
|---|---|---|
| LLM | Kimi-K2.6 via Nebius (default, parameterized) | assignment default; `model` param makes it an experiment variable |
| Object storage | MinIO (dev) → Nebius OS (optional prod) | same boto3 code path; repo stays reproducible for outsiders |
| Airflow executor (compose) | LocalExecutor + postgres | one VM, no need for Celery/redis; less to debug |
| MLflow backend | sqlite (or postgres) via compose volume | tracking scale is tiny; durability via volume |
| Orchestration↔execution boundary | CLI subprocess → DockerOperator | PLAN §1; the assignment's easy→production path without a rewrite |
