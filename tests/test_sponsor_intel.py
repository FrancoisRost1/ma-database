"""Tests: sponsor intelligence analytics."""
from ma.analytics import sponsor_intel


def test_sponsor_rankings_not_empty(seeded_db, config):
    df = sponsor_intel.sponsor_rankings()
    assert not df.empty
    assert "sponsor_name" in df.columns
    assert "deal_count" in df.columns


def test_sponsor_rankings_sorted_by_deal_count(seeded_db, config):
    df = sponsor_intel.sponsor_rankings(top_n=10)
    counts = df["deal_count"].tolist()
    assert counts == sorted(counts, reverse=True)


def test_sponsor_sector_heatmap(seeded_db, config):
    df = sponsor_intel.sponsor_sector_heatmap()
    assert "sponsor_name" in df.columns


def test_sponsor_entry_multiples(seeded_db, config):
    df = sponsor_intel.sponsor_entry_multiples()
    if not df.empty:
        assert "avg_ev_to_ebitda" in df.columns


def test_most_active_sponsor_returns_string(seeded_db, config):
    result = sponsor_intel.most_active_sponsor()
    assert isinstance(result, str)
    assert result != ""


def test_sponsor_deal_trend(seeded_db, config):
    df = sponsor_intel.sponsor_deal_trend(top_n_sponsors=3)
    if not df.empty:
        assert "year" in df.columns
        assert "deal_count" in df.columns
