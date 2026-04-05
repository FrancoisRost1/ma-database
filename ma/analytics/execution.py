"""
Execution analytics — deal status breakdown, time-to-close, completion rates.
Answers: how long do deals take? What share are terminated?
Assumption: time-to-close = closing_date − announcement_date; no regulatory delay adjustment.
"""
import pandas as pd
import numpy as np
from ma.db import queries
from ma.db.engine import get_connection


def time_to_close_distribution(filters: dict = None) -> pd.DataFrame:
    """
    Distribution of days from announcement to close for completed deals.
    Returns a DataFrame with columns: deal_id, target_name, days_to_close, deal_type, sector_name.
    """
    conn = get_connection()
    where, params = queries._build_where(filters or {})
    w = queries._and_condition(where, "closing_date IS NOT NULL AND deal_status = 'closed'")
    sql = f"""
        SELECT deal_id, target_name, acquirer_name, deal_type, sector_name,
               announcement_date, closing_date,
               DATEDIFF('day', announcement_date, closing_date) AS days_to_close
        FROM v_deals_flat {w}
        ORDER BY days_to_close
    """
    df = conn.execute(sql, params).df()
    return df.dropna(subset=["days_to_close"])


def time_to_close_stats(filters: dict = None) -> dict:
    """
    Summary statistics for time-to-close:
    median, mean, p25, p75 in calendar days.
    """
    df = time_to_close_distribution(filters)
    if df.empty:
        return {}
    col = df["days_to_close"]
    return {
        "median_days": float(col.median()),
        "mean_days": float(col.mean()),
        "p25_days": float(col.quantile(0.25)),
        "p75_days": float(col.quantile(0.75)),
        "count": len(df),
    }


def deal_status_summary(filters: dict = None) -> pd.DataFrame:
    """
    Count of deals by status across the filtered universe.
    Used for the status breakdown stacked bar chart.
    """
    df = queries.get_all_deals(filters)
    if df.empty:
        return pd.DataFrame()
    return df.groupby("deal_status").size().reset_index(name="deal_count")


def completion_rate_by_deal_type(filters: dict = None) -> pd.DataFrame:
    """
    Completion rate (% closed of decided deals) broken down by deal_type.
    Useful for understanding which transaction structures face most regulatory risk.
    """
    df = queries.get_all_deals(filters)
    if df.empty:
        return pd.DataFrame()

    result = []
    for deal_type, group in df.groupby("deal_type"):
        closed = int((group["deal_status"] == "closed").sum())
        terminated = int((group["deal_status"] == "terminated").sum())
        decided = closed + terminated
        rate = (closed / decided * 100) if decided > 0 else None
        result.append({
            "deal_type": deal_type,
            "total": len(group),
            "closed": closed,
            "terminated": terminated,
            "completion_rate_pct": round(rate, 1) if rate else None,
        })

    return pd.DataFrame(result).sort_values("total", ascending=False)


def avg_time_to_close_by_sector(filters: dict = None) -> pd.DataFrame:
    """Average days to close by sector."""
    df = time_to_close_distribution(filters)
    if df.empty:
        return pd.DataFrame()
    return (
        df.groupby("sector_name")["days_to_close"]
        .agg(avg_days="mean", median_days="median", count="count")
        .reset_index()
        .sort_values("avg_days", ascending=False)
    )
