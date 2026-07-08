"""Layer 3: the CLI — the single entry point every orchestrator uses.

Each subcommand maps 1:1 to a DAG task. Contract: exactly one JSON line on
stdout (machine-parseable by the DAG); all tool/log noise goes to stderr.
This module is the only place services are imported and composed (PLAN §4).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline import agent_runner, evaluator
from pipeline.artifacts import RunPaths, init_run_dir, load_config
from pipeline.config import PARAM_DEFAULTS, resolve_config


def build_parser() -> argparse.ArgumentParser:
    """Assemble the argparse tree. prepare-run's flags are generated from
    PARAM_DEFAULTS (SPEC C1): add a knob to the dict and the CLI grows it."""
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.cli",
        description="Coding-agent evaluation pipeline steps.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare-run", help="resolve config and create runs/<run-id>/"
    )
    for key, default in PARAM_DEFAULTS.items():
        prepare.add_argument(
            "--" + key.replace("_", "-"),
            type=type(default),
            default=None,
            help=f"default: {default}",
        )
    prepare.add_argument(
        "--run-id", default=None, help="override the generated run id (reruns)"
    )

    for name, help_text in (
        ("run-agent", "run the mini-swe-agent batch for a prepared run"),
        ("run-eval", "run the SWE-bench harness on the run's predictions"),
    ):
        step = subparsers.add_parser(name, help=help_text)
        step.add_argument("--run-dir", required=True, help="runs/<run-id> directory")

    return parser


def _paths_from(run_dir: str) -> RunPaths:
    return RunPaths(root=Path(run_dir).resolve())


def main(argv: list[str] | None = None) -> None:
    """Parse args, dispatch, print the one-line JSON result."""
    try:  # optional convenience: pick up .env when run from the project root
        from dotenv import load_dotenv

        load_dotenv(override=False)
    except ImportError:
        pass

    args = build_parser().parse_args(argv)

    if args.command == "prepare-run":
        overrides = {key: getattr(args, key) for key in PARAM_DEFAULTS}
        config = resolve_config(overrides, run_id=args.run_id)
        paths = init_run_dir(config)
        result = {"run_id": config.run_id, "run_dir": str(paths.root)}
    elif args.command == "run-agent":
        paths = _paths_from(args.run_dir)
        result = agent_runner.run_agent(load_config(paths), paths)
    elif args.command == "run-eval":
        paths = _paths_from(args.run_dir)
        result = evaluator.run_eval(load_config(paths), paths)
    else:  # pragma: no cover - argparse enforces the choices
        raise SystemExit(f"unknown command {args.command!r}")

    print(json.dumps(result))


if __name__ == "__main__":
    main()
