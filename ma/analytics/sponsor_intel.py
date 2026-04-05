"""
Sponsor intelligence analytics — rankings, sector preferences, entry multiples.
Key question: which sponsors are most active, and what do they target?
Assumption: consortium deals attributed to primary sponsor only (Phase 1).
"""
import pandas as pd
import numpy as np
from ma.db import queries


def sponsor_rankings(filters: dict = None, top_n: int = 15) -> pd.DataFrame:
    """
    Ranked table of sponsors by deal count, total deal value, and average deal size.
    Sorted by deal count descending.
    """
    df = queries.get_sponsor_rankings(filters, top_n=top_n)
    if df.empty:
        return df
    df["avg_deal_size_usd"] = df["avg_deal_size_usd"].round(1)
    df["avg_ev_to_ebitda"] = df["avg_ev_to_ebitda"].round(2)
    return df


def sponsor_sector_heatmap(filters: dict = None, top_n_sponsors: int = 12) -> pd.DataFrame:
    """
    Pivot table of deal count by sponsor × sector.
    Suitable for a Plotly heatmap where rows=sponsors, columns=sectors.
    """
    df = queries.get_sponsor_sector_heatmap(filters, top_n_sponsors=top_n_sponsors)
    if df.empty:
        return df
    pivot = df.pivot_table(
        index="sponsor_name", columns="sector_name", values="deal_count", fill_value=0
    ).reset_index()
    return pivot


def sponsor_deal_trend(filters: dict = None, top_n_sponsors: int = 5) -> pd.DataFrame:
    """Annual deal count for top N sponsors — line chart."""
    return queries.get_sponsor_deal_trend(filters, top_n_sponsors=top_n_sponsors)


def sponsor_entry_multiples(filters: dict = None, top_n: int = 15) -> pd.DataFrame:
    """
    Average entry EV/EBITDA per sponsor (where disclosed).
    Answers: which sponsors pay the highest multiples?
    """
    df = queries.get_sponsor_rankings(filters, top_n=top_n)
    if df.empty:
        return df
    return df[["sponsor_name", "avg_ev_to_ebitda", "deal_count"]].dropna(
        subset=["avg_ev_to_ebitda"]
    ).sort_values("avg_ev_to_ebitda", ascending=False)


def top_sponsors_by_value(filters: dict = None, top_n: int = 10) -> pd.DataFrame:
    """Sponsors ranked by total disclosed deal value."""
    df = queries.get_sponsor_rankings(filters, top_n=top_n)
    if df.empty:
        return df
    return df.sort_values("total_deal_value_usd", ascending=False).head(top_n)


def most_active_sponsor(filters: dict = None) -> str:
    """Return the single most active sponsor by deal count."""
    df = sponsor_rankings(filters, top_n=1)
    if df.empty:
        return "N/A"
    return df.iloc[0]["sponsor_name"]


def sponsor_sector_preference(sponsor_name: str, filters: dict = None) -> pd.DataFrame:
    """
    Return deal count per sector for a single named sponsor.
    Used for drill-down in the deal explorer.
    """
    f = dict(filters or {})
    f["acquirer_names"] = [sponsor_name]
    f["acquirer_types"] = ["sponsor"]
    df = queries.get_deal_count_by_sector(f, top_n=20)
    return df
