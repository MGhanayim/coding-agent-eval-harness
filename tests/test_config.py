"""Unit tests for pipeline.config (Layer 0)."""
import dataclasses
import json
from datetime import datetime, timezone

import pytest

from pipeline.config import (
    PARAM_DEFAULTS,
    RunConfig,
    SUBSET_DATASETS,
    generate_run_id,
    resolve_config,
)

FIXED_NOW = datetime(2026, 7, 2, 14, 25, 30, tzinfo=timezone.utc)


def test_defaults_fill_when_no_overrides():
    config = resolve_config(now=FIXED_NOW)
    assert config.split == PARAM_DEFAULTS["split"]
    assert config.subset == PARAM_DEFAULTS["subset"]
    assert config.workers == PARAM_DEFAULTS["workers"]
    assert config.model == PARAM_DEFAULTS["model"]


def test_overrides_win_and_none_means_not_provided():
    config = resolve_config({"workers": 8, "split": None}, now=FIXED_NOW)
    assert config.workers == 8
    assert config.split == PARAM_DEFAULTS["split"]


def test_unknown_override_key_fails_loudly():
    with pytest.raises(ValueError, match="unknown parameters"):
        resolve_config({"modle": "typo"}, now=FIXED_NOW)


def test_unknown_subset_fails_loudly():
    with pytest.raises(ValueError, match="unknown subset"):
        resolve_config({"subset": "does-not-exist"}, now=FIXED_NOW)


def test_run_id_format_matches_plan():
    assert (
        generate_run_id(FIXED_NOW, "verified", "0:3")
        == "20260702T142530__verified__0-3"
    )


def test_run_id_generation_is_deterministic():
    a = resolve_config(now=FIXED_NOW)
    b = resolve_config(now=FIXED_NOW)
    assert a.run_id == b.run_id == "20260702T142530__verified__0-3"


def test_explicit_run_id_wins():
    config = resolve_config(run_id="my-rerun", now=FIXED_NOW)
    assert config.run_id == "my-rerun"


def test_dataset_name_derived_from_subset():
    config = resolve_config({"subset": "lite"}, now=FIXED_NOW)
    assert config.dataset_name == SUBSET_DATASETS["lite"]


def test_json_round_trip_is_lossless():
    original = resolve_config(now=FIXED_NOW)
    restored = RunConfig.from_json(original.to_json())
    assert restored == original


def test_config_json_has_all_plan_fields():
    payload = json.loads(resolve_config(now=FIXED_NOW).to_json())
    assert set(payload) == {
        "run_id", "created_at", "split", "subset", "workers", "model",
        "task_slice", "cost_limit", "dataset_name", "package_versions",
    }


def test_package_versions_are_recorded():
    config = resolve_config(now=FIXED_NOW)
    assert config.package_versions.get("mini-swe-agent") not in (None, "unknown")
    assert config.package_versions.get("swebench") not in (None, "unknown")


def test_config_is_frozen():
    config = resolve_config(now=FIXED_NOW)
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.workers = 99  # type: ignore[misc]
