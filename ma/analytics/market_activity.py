"""
Market activity analytics — deal volume trends, sector heatmaps,
sponsor vs strategic mix, and deal status breakdowns.
Key question: where is buyout activity accelerating or slowing?
"""
import pandas as pd
import numpy as np
from ma.db import queries


def deal_count_over_time(filters: dict = None) -> pd.DataFrame:
    """Annual deal count time series."""
    return queries.get_deal_count_by_year(filters)


def deal_value_over_time(filters: dict = None) -> pd.DataFrame:
    """Annual total deal value (millions USD) time series."""
    return queries.get_deal_value_by_year(filters)


def sector_activity_heatmap(filters: dict = None) -> pd.DataFrame:
    """
    Pivot table of deal count by sector × year.
    Pivoted for direct use in a Plotly heatmap: rows=sectors, columns=years.
    """
    df = queries.get_sector_activity_heatmap(filters)
    if df.empty:
        return df
    pivot = df.pivot_table(
        index="sector_name", columns="year", values="deal_count", fill_value=0
    ).reset_index()
    return pivot


def sponsor_vs_strategic_trend(filters: dict = None) -> pd.DataFrame:
    """Annual sponsor vs strategic deal count for stacked/grouped bar chart."""
    return queries.get_sponsor_vs_strategic_trend(filters)


def deal_status_breakdown(filters: dict = None) -> pd.DataFrame:
    """Deal count by status (closed/announced/terminated) over time."""
    return queries.get_deal_status_breakdown(filters)


def sector_value_treemap(filters: dict = None) -> pd.DataFrame:
    """Total deal value and count by sector for treemap visualization."""
    return queries.get_sector_value_treemap(filters)


def deal_completion_rate(filters: dict = None) -> dict:
    """
    Compute deal completion statistics:
    - total, closed, terminated, completion_rate_pct
    Used in the M&A Market Snapshot memo.
    """
    df = queries.get_all_deals(filters)
    if df.empty:
        return {"total": 0, "closed": 0, "terminated": 0, "completion_rate_pct": None}

    total = len(df)
    closed = int((df["deal_status"] == "closed").sum())
    terminated = int((df["deal_status"] == "terminated").sum())
    decidable = closed + terminated
    rate = (closed / decidable * 100) if decidable > 0 else None

    return {
        "total": total,
        "closed": closed,
        "terminated": terminated,
        "completion_rate_pct": round(rate, 1) if rate else None,
    }


def top_sectors_by_period(filters: dict = None, top_n: int = 3) -> "list[str]":
    """Return the top N most active sectors by deal count in the filtered period."""
    df = queries.get_deal_count_by_sector(filters, top_n=top_n)
    if df.empty:
        return []
    # Drop NaN values and keep only string sector names (nulls arise from deals with no sector)
    return [s for s in df["sector_name"].tolist() if isinstance(s, str)]


def quarter_label(year: int, quarter: int) -> str:
    """Format year+quarter into a label string like '2022 Q1'."""
    return f"{year} Q{quarter}"
