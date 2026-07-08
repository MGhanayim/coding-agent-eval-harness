"""Unit tests for pipeline.cli (parser generation + prepare-run wiring)."""
import json

from pipeline.cli import build_parser, main
from pipeline.config import PARAM_DEFAULTS


def test_prepare_run_flags_generated_from_param_defaults():
    args = build_parser().parse_args(["prepare-run"])
    for key in PARAM_DEFAULTS:
        assert getattr(args, key) is None  # None = "not provided", defaults fill later


def test_prepare_run_flag_types_follow_defaults():
    args = build_parser().parse_args(
        ["prepare-run", "--workers", "8", "--cost-limit", "1.5"]
    )
    assert args.workers == 8
    assert args.cost_limit == 1.5


def test_prepare_run_creates_run_dir_and_prints_json(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("RUNS_ROOT", str(tmp_path))
    main(["prepare-run", "--task-slice", "0:1", "--run-id", "cli-test-run"])
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["run_id"] == "cli-test-run"
    assert (tmp_path / "cli-test-run" / "config.json").is_file()
