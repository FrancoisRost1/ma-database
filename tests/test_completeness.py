"""Tests: completeness scoring logic."""
from ma.scoring.completeness import compute_completeness, quality_tier


def _full_row():
    return {
        "announcement_date": "2022-01-15",
        "acquirer_party_id": "party-123",
        "target_name": "Test Co",
        "deal_type": "lbo",
        "sector_id": "sector-123",
        "deal_value_usd": 5000.0,
        "deal_status": "closed",
        "data_origin": "real",
        "ev_to_ebitda": 12.0,
        "ev_to_revenue": 3.5,
        "premium_paid_pct": 25.0,
        "acquirer_type": "sponsor",
        "closing_date": "2022-06-01",
        "geography": "US",
        "financing_structure_text": "TLB + equity",
        "leverage_multiple": 5.0,
        "hostile_or_friendly": "friendly",
        "notes": "Test notes",
        "source_url": "https://example.com",
        "sub_industry": "Application Software",
    }


def test_full_row_high_completeness(config):
    score = compute_completeness(_full_row(), config)
    assert score >= 80, f"Expected high completeness, got {score}"


def test_empty_row_low_completeness(config):
    score = compute_completeness({}, config)
    assert score == 0.0


def test_partial_row_medium_completeness(config):
    # Only tier-1 fields filled
    row = {
        "announcement_date": "2022-01-15",
        "acquirer_party_id": "p1",
        "target_name": "Test",
        "deal_type": "lbo",
        "sector_id": "s1",
        "deal_value_usd": 1000.0,
        "deal_status": "closed",
        "data_origin": "real",
    }
    score = compute_completeness(row, config)
    assert 0 < score < 100


def test_quality_tier_labels(config):
    assert quality_tier(90.0, config) == "High"
    assert quality_tier(65.0, config) == "Medium"
    assert quality_tier(30.0, config) == "Low"


def test_none_values_treated_as_missing(config):
    row = {k: None for k in _full_row()}
    score = compute_completeness(row, config)
    assert score == 0.0


def test_score_between_0_and_100(config):
    score = compute_completeness(_full_row(), config)
    assert 0 <= score <= 100
