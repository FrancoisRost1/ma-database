"""Tests: valuation analytics correctness."""
from ma.analytics import valuation


def test_ev_ebitda_by_sector_returns_dataframe(seeded_db, config):
    df = valuation.ev_ebitda_by_sector()
    assert "ev_to_ebitda" in df.columns
    assert "sector_name" in df.columns
    assert not df.empty


def test_no_null_ev_ebitda_in_output(seeded_db, config):
    df = valuation.ev_ebitda_by_sector()
    assert df["ev_to_ebitda"].isna().sum() == 0


def test_sector_valuation_stats_has_median(seeded_db, config):
    df = valuation.sector_valuation_stats()
    assert "median" in df.columns
    assert not df.empty


def test_ev_revenue_by_sector(seeded_db, config):
    df = valuation.ev_revenue_by_sector()
    assert not df.empty


def test_sponsor_vs_strategic_multiples(seeded_db, config):
    df = valuation.sponsor_vs_strategic_multiples()
    assert not df.empty
    assert "acquirer_type" in df.columns


def test_median_ev_ebitda_by_sector_year(seeded_db, config):
    df = valuation.median_ev_ebitda_by_sector_year()
    assert "year" in df.columns
    assert "median_ev_to_ebitda" in df.columns
