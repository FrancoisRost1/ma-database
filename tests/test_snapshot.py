"""Tests: M&A Market Snapshot memo generation."""
from ma.analytics.snapshot import generate_snapshot


def test_snapshot_returns_string(seeded_db, config):
    memo = generate_snapshot(config=config)
    assert isinstance(memo, str)
    assert len(memo) > 50


def test_snapshot_contains_key_sections(seeded_db, config):
    memo = generate_snapshot(config=config)
    assert "Deal Activity" in memo or "M&A Market Snapshot" in memo


def test_snapshot_empty_filters(seeded_db, config):
    memo = generate_snapshot(filters={"year_start": 1900, "year_end": 1901}, config=config)
    assert "No deals" in memo or len(memo) > 0


def test_snapshot_with_real_only_filter(seeded_db, config):
    memo = generate_snapshot(filters={"data_origin": "real"}, config=config)
    assert isinstance(memo, str)
