"""Unit tests for pipeline.artifacts (Layer 1)."""
from datetime import datetime, timezone

import pytest

from pipeline.artifacts import RunPaths, init_run_dir, load_config
from pipeline.config import resolve_config

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)


def test_all_paths_derive_from_one_root(tmp_path):
    paths = RunPaths.for_run("some-run", root=tmp_path)
    for p in (
        paths.config_path,
        paths.preds_path,
        paths.trajectories_dir,
        paths.eval_logs_dir,
        paths.eval_reports_dir,
        paths.metrics_path,
        paths.manifest_path,
    ):
        assert p.is_relative_to(tmp_path / "some-run")
    assert paths.run_id == "some-run"


def test_runs_root_env_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNS_ROOT", str(tmp_path / "elsewhere"))
    paths = RunPaths.for_run("r1")
    assert paths.root == tmp_path / "elsewhere" / "r1"


def test_init_run_dir_creates_spec_21_tree(tmp_path):
    config = resolve_config(now=FIXED_NOW)
    paths = init_run_dir(config, root=tmp_path)
    assert paths.config_path.is_file()
    assert paths.trajectories_dir.is_dir()
    assert paths.eval_logs_dir.is_dir()
    assert paths.eval_reports_dir.is_dir()


def test_init_run_dir_round_trips_config(tmp_path):
    config = resolve_config(now=FIXED_NOW)
    paths = init_run_dir(config, root=tmp_path)
    assert load_config(paths) == config


def test_duplicate_run_id_fails_instead_of_overwriting(tmp_path):
    config = resolve_config(now=FIXED_NOW)
    init_run_dir(config, root=tmp_path)
    with pytest.raises(FileExistsError):
        init_run_dir(config, root=tmp_path)
