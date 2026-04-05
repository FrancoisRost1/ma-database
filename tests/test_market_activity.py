"""Tests: market activity analytics."""
from ma.analytics import market_activity


def test_deal_count_over_time(seeded_db, config):
    df = market_activity.deal_count_over_time()
    assert not df.empty
    assert "year" in df.columns
    assert "deal_count" in df.columns
    assert (df["deal_count"] > 0).all()


def test_deal_value_over_time(seeded_db, config):
    df = market_activity.deal_value_over_time()
    assert "total_value_usd" in df.columns


def test_sector_activity_heatmap_pivot(seeded_db, config):
    df = market_activity.sector_activity_heatmap()
    assert "sector_name" in df.columns
    # Should have year columns as well
    year_cols = [c for c in df.columns if str(c).isdigit() or isinstance(c, (int, float))]
    assert len(year_cols) > 0


def test_sponsor_vs_strategic_trend(seeded_db, config):
    df = market_activity.sponsor_vs_strategic_trend()
    assert "acquirer_type" in df.columns
    assert "deal_count" in df.columns


def test_deal_completion_rate(seeded_db, config):
    result = market_activity.deal_completion_rate()
    assert "total" in result
    assert result["total"] > 0


def test_top_sectors_by_period(seeded_db, config):
    sectors = market_activity.top_sectors_by_period(top_n=3)
    assert len(sectors) <= 3


def test_deal_status_breakdown(seeded_db, config):
    df = market_activity.deal_status_breakdown()
    assert "deal_status" in df.columns
