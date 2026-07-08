"""Unit tests for pipeline.agent_runner (pure builders + relocation/validation)."""
import dataclasses
import json
from datetime import datetime, timezone

import pytest

from pipeline.agent_runner import (
    build_agent_command,
    refresh_package_versions,
    relocate_preds,
    validate_preds,
)
from pipeline.artifacts import init_run_dir, load_config
from pipeline.config import resolve_config

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)


def _run(tmp_path, **overrides):
    config = resolve_config(overrides or None, now=FIXED_NOW)
    paths = init_run_dir(config, root=tmp_path)
    return config, paths


def test_command_mirrors_batch_script(tmp_path):
    config, paths = _run(tmp_path, workers=5, task_slice="0:3")
    command = build_agent_command(config, paths)
    assert command[:2] == ["mini-extra", "swebench"]
    for flag, value in [
        ("--subset", "verified"),
        ("--split", "test"),
        ("--model", config.model),
        ("--slice", "0:3"),
        ("--workers", "5"),
        ("-o", str(paths.trajectories_dir)),
    ]:
        index = command.index(flag)
        assert command[index + 1] == value


def test_cost_limit_zero_is_passed_explicitly(tmp_path):
    # The packaged swebench.yaml sets its own cost_limit (3.0); only an
    # explicit agent.cost_limit=0 genuinely disables the ceiling.
    config, paths = _run(tmp_path)
    command = build_agent_command(config, paths)
    assert "swebench.yaml" in command
    assert "agent.cost_limit=0.0" in command


def test_positive_cost_limit_rides_in_via_config_override(tmp_path):
    config, paths = _run(tmp_path, cost_limit=2.5)
    command = build_agent_command(config, paths)
    assert "swebench.yaml" in command
    assert "agent.cost_limit=2.5" in command


def test_builder_is_pure(tmp_path):
    config, paths = _run(tmp_path)
    assert build_agent_command(config, paths) == build_agent_command(config, paths)


def test_relocate_copies_preds_keeping_resume_marker(tmp_path):
    # COPY, not move: mini-extra's skip-existing resume reads the copy
    # inside trajectories/ — removing it would re-run the whole batch.
    _, paths = _run(tmp_path)
    source = paths.trajectories_dir / "preds.json"
    source.write_text(json.dumps({"x": {"model_patch": "diff"}}))
    relocate_preds(paths)
    assert source.exists()
    assert paths.preds_path.is_file()


def test_refresh_package_versions_repairs_unknown(tmp_path):
    config, paths = _run(tmp_path)
    broken = dataclasses.replace(
        config, package_versions={"mini-swe-agent": "unknown", "swebench": "unknown"}
    )
    paths.config_path.write_text(broken.to_json())

    fixed = refresh_package_versions(broken, paths)

    assert "unknown" not in fixed.package_versions.values()
    assert load_config(paths).package_versions == fixed.package_versions


def test_refresh_package_versions_noop_when_known(tmp_path):
    config, paths = _run(tmp_path)
    assert refresh_package_versions(config, paths) is config


def test_validate_counts_non_empty_patches(tmp_path):
    _, paths = _run(tmp_path)
    paths.preds_path.write_text(
        json.dumps(
            {
                "a": {"model_patch": "diff --git ..."},
                "b": {"model_patch": ""},
            }
        )
    )
    assert validate_preds(paths) == 1


def test_validate_fails_on_missing_file(tmp_path):
    _, paths = _run(tmp_path)
    with pytest.raises(RuntimeError, match="no predictions file"):
        validate_preds(paths)


def test_validate_fails_when_every_patch_is_empty(tmp_path):
    _, paths = _run(tmp_path)
    paths.preds_path.write_text(json.dumps({"a": {"model_patch": ""}}))
    with pytest.raises(RuntimeError, match="empty patches"):
        validate_preds(paths)
