"""
Valuation analytics — EV/EBITDA and EV/Revenue distributions by sector,
premium analysis for public targets, and valuation regime shifts over time.
Key question: which sectors trade at highest multiples, and are they expanding?
"""
import pandas as pd
import numpy as np
from ma.db import queries


def ev_ebitda_by_sector(filters: dict = None) -> pd.DataFrame:
    """
    EV/EBITDA distribution by sector — used for box plots.
    Returns raw observation-level data (each row = one deal).
    """
    df = queries.get_ev_ebitda_by_sector(filters)
    return df.dropna(subset=["ev_to_ebitda"])


def ev_revenue_by_sector(filters: dict = None) -> pd.DataFrame:
    """EV/Revenue distribution by sector for box plots."""
    df = queries.get_ev_revenue_by_sector(filters)
    return df.dropna(subset=["ev_to_revenue"])


def sector_valuation_stats(filters: dict = None) -> pd.DataFrame:
    """
    Summary statistics per sector: median, mean, 25th/75th percentile EV/EBITDA.
    Used for ranked bar comparison and tooltip overlays.
    """
    df = ev_ebitda_by_sector(filters)
    if df.empty:
        return pd.DataFrame()

    stats = (
        df.groupby("sector_name")["ev_to_ebitda"]
        .agg(
            count="count",
            median=lambda x: x.median(),
            mean="mean",
            p25=lambda x: x.quantile(0.25),
            p75=lambda x: x.quantile(0.75),
        )
        .reset_index()
        .sort_values("median", ascending=False)
    )
    return stats


def premium_distribution(filters: dict = None) -> pd.DataFrame:
    """
    Premium paid % for public targets.
    Only meaningful for deals where target_status='public'.
    """
    df = queries.get_premium_distribution(filters)
    return df.dropna(subset=["premium_paid_pct"])


def median_ev_ebitda_by_sector_year(filters: dict = None) -> pd.DataFrame:
    """
    Valuation regime shift chart: median EV/EBITDA by sector × year.
    Allows visual detection of multiple expansion or compression cycles.
    """
    return queries.get_median_ev_ebitda_by_sector_year(filters)


def sponsor_vs_strategic_multiples(filters: dict = None) -> pd.DataFrame:
    """
    Average and median EV/EBITDA split by acquirer type.
    Canonical answer to: 'Do PE sponsors pay more or less than strategics?'
    """
    df = queries.get_sponsor_vs_strategic_multiples(filters)
    return df


def valuation_regime_comparison(filters: dict = None) -> dict:
    """
    Compare median EV/EBITDA in the current filter window vs the same-length prior period.
    Returns a dict with current_median, prior_median, change_pct, direction.
    Used in the M&A Market Snapshot memo.
    Assumption: prior period is defined as same number of years immediately before start.
    """
    df = queries.get_all_deals(filters)
    if df.empty or "ev_to_ebitda" not in df.columns:
        return {}

    df = df.dropna(subset=["ev_to_ebitda", "announcement_year"])
    if df.empty:
        return {}

    current_years = sorted(df["announcement_year"].unique())
    if len(current_years) < 2:
        return {}

    n_years = len(current_years)
    prior_start = int(min(current_years)) - n_years
    prior_end = int(min(current_years)) - 1

    prior_filters = dict(filters or {})
    prior_filters["year_start"] = prior_start
    prior_filters["year_end"] = prior_end
    prior_df = queries.get_all_deals(prior_filters)

    current_median = float(df["ev_to_ebitda"].median())
    prior_median = float(prior_df["ev_to_ebitda"].median()) if not prior_df.empty else None

    change_pct = None
    direction = "stable"
    if prior_median and prior_median > 0:
        change_pct = ((current_median - prior_median) / prior_median) * 100
        if change_pct > 5:
            direction = "expansion"
        elif change_pct < -5:
            direction = "compression"

    return {
        "current_median": current_median,
        "prior_median": prior_median,
        "change_pct": change_pct,
        "direction": direction,
    }
