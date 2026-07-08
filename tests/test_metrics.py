"""Unit tests for pipeline.metrics — verified against the instructor's
real sample summary, not synthetic data."""
import json
from pathlib import Path

import pytest

from pipeline.metrics import summarize_counts

SAMPLE_SUMMARY = Path("sample/nebius__moonshotai__Kimi-K2.6.test.json")


def test_sample_summary_distills_to_expected_metrics():
    metrics = summarize_counts(json.loads(SAMPLE_SUMMARY.read_text()))
    assert metrics == {
        "total_instances": 500,
        "submitted_instances": 3,
        "completed_instances": 3,
        "resolved_instances": 1,
        "unresolved_instances": 2,
        "empty_patch_instances": 0,
        "error_instances": 0,
        "resolve_rate": 0.333,
    }


def test_zero_submitted_means_zero_rate_not_crash():
    summary = {key: 0 for key in (
        "total_instances", "submitted_instances", "completed_instances",
        "resolved_instances", "unresolved_instances",
        "empty_patch_instances", "error_instances",
    )}
    assert summarize_counts(summary)["resolve_rate"] == 0.0


def test_missing_counter_fails_loudly():
    with pytest.raises(KeyError):
        summarize_counts({"resolved_instances": 1})
