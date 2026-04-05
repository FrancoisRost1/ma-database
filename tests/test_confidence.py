"""Tests: confidence scoring rules."""
from ma.scoring.confidence import compute_confidence, confidence_label


def test_real_with_source_url(config):
    row = {"data_origin": "real", "source_url": "https://example.com"}
    score = compute_confidence(row, 85.0, config)
    assert score == 1.0


def test_real_without_source_url(config):
    row = {"data_origin": "real", "source_url": None}
    score = compute_confidence(row, 85.0, config)
    assert score == 0.8


def test_synthetic_record(config):
    row = {"data_origin": "synthetic", "source_url": None}
    score = compute_confidence(row, 65.0, config)
    assert score == 0.5


def test_low_completeness_cap(config):
    row = {"data_origin": "real", "source_url": "https://example.com"}
    score = compute_confidence(row, 20.0, config)  # below 30% threshold
    assert score <= 0.3


def test_confidence_labels():
    assert confidence_label(1.0) == "High"
    assert confidence_label(0.8) == "Medium-High"
    assert confidence_label(0.5) == "Medium"
    assert confidence_label(0.3) == "Low"
    assert confidence_label(0.1) == "Very Low"


def test_unknown_origin_low_confidence(config):
    row = {"data_origin": "unknown", "source_url": None}
    score = compute_confidence(row, 70.0, config)
    assert score == 0.3
