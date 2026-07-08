"""Unit tests for pipeline.evaluator (pure builder + output reshaping)."""
import json
import sys
from datetime import datetime, timezone

import pytest

from pipeline.artifacts import init_run_dir
from pipeline.config import resolve_config
from pipeline.evaluator import build_eval_command, relocate_eval_outputs, validate_eval

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)
MODEL_SLUG = "nebius__moonshotai__Kimi-K2.6"


def _run(tmp_path, **overrides):
    config = resolve_config(overrides or None, now=FIXED_NOW)
    paths = init_run_dir(config, root=tmp_path)
    return config, paths


def test_command_targets_the_runs_predictions(tmp_path):
    config, paths = _run(tmp_path, workers=2)
    command = build_eval_command(config, paths)
    assert command[:3] == [sys.executable, "-m", "swebench.harness.run_evaluation"]
    for flag, value in [
        ("--dataset_name", "princeton-nlp/SWE-bench_Verified"),
        ("--split", "test"),
        ("--predictions_path", str(paths.preds_path)),
        ("--max_workers", "2"),
        ("--run_id", config.run_id),
    ]:
        index = command.index(flag)
        assert command[index + 1] == value


def _fake_harness_output(config, paths, instance="astropy__astropy-12907"):
    """Reproduce the raw layout the harness leaves in cwd=run-eval/."""
    instance_dir = (
        paths.eval_logs_dir / "run_evaluation" / config.run_id / MODEL_SLUG / instance
    )
    instance_dir.mkdir(parents=True)
    (instance_dir / "report.json").write_text(
        json.dumps({instance: {"resolved": True}})
    )
    (instance_dir / "patch.diff").write_text("diff --git ...")
    (paths.eval_dir / f"{MODEL_SLUG}.{config.run_id}.json").write_text(
        json.dumps({"resolved_instances": 1})
    )


def test_relocation_produces_spec_21_shape(tmp_path):
    config, paths = _run(tmp_path)
    _fake_harness_output(config, paths)
    relocate_eval_outputs(config, paths)

    instance_logs = paths.eval_logs_dir / "astropy__astropy-12907"
    assert (instance_logs / "report.json").is_file()
    assert (instance_logs / "patch.diff").is_file()
    assert not (paths.eval_logs_dir / "run_evaluation").exists()
    assert (paths.eval_reports_dir / "summary.json").is_file()
    assert (paths.eval_reports_dir / "astropy__astropy-12907.json").is_file()


def test_relocation_is_retry_idempotent(tmp_path):
    # An Airflow retry re-runs the harness AND relocation over the same run
    # dir — the second pass must overwrite, never collide (ENOTEMPTY).
    config, paths = _run(tmp_path)
    _fake_harness_output(config, paths)
    relocate_eval_outputs(config, paths)
    _fake_harness_output(config, paths)
    relocate_eval_outputs(config, paths)

    assert (paths.eval_logs_dir / "astropy__astropy-12907" / "report.json").is_file()
    assert (paths.eval_reports_dir / "summary.json").is_file()


def test_relocation_strips_dangling_symlinks(tmp_path):
    config, paths = _run(tmp_path)
    _fake_harness_output(config, paths)
    instance_dir = (
        paths.eval_logs_dir / "run_evaluation" / config.run_id / MODEL_SLUG
        / "astropy__astropy-12907"
    )
    (instance_dir / "image_build_dir").symlink_to("/nonexistent/absolute/path")
    relocate_eval_outputs(config, paths)

    relocated = paths.eval_logs_dir / "astropy__astropy-12907"
    assert not any(p.is_symlink() for p in relocated.iterdir())


def test_relocation_parks_build_logs_outside_logs(tmp_path):
    config, paths = _run(tmp_path)
    _fake_harness_output(config, paths)
    build_log = paths.eval_logs_dir / "build_images" / "env" / "build.log"
    build_log.parent.mkdir(parents=True)
    build_log.write_text("...")
    relocate_eval_outputs(config, paths)

    assert not (paths.eval_logs_dir / "build_images").exists()
    assert (paths.eval_dir / "build_images" / "env" / "build.log").is_file()


def test_validate_requires_summary(tmp_path):
    config, paths = _run(tmp_path)
    with pytest.raises(RuntimeError, match="no summary"):
        validate_eval(paths)
    _fake_harness_output(config, paths)
    relocate_eval_outputs(config, paths)
    validate_eval(paths)  # should not raise
