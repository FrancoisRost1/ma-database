"""
Formatting helpers — currency display, date parsing, label normalization.
Used by both the dashboard and export layers.
"""
import pandas as pd
import numpy as np
from datetime import date
from typing import Optional


def fmt_currency(value_m: Optional[float], decimals: int = 1, suffix: str = "B") -> str:
    """
    Format a deal value (in millions USD) into a human-readable string.
    Assumption: values are in millions USD as stored in the database.
    """
    if value_m is None or (isinstance(value_m, float) and np.isnan(value_m)):
        return "N/A"
    if suffix == "B":
        return f"${value_m / 1_000:.{decimals}f}B"
    return f"${value_m:,.{decimals}f}M"


def fmt_multiple(value: Optional[float], suffix: str = "x") -> str:
    """Format an EV/EBITDA or EV/Revenue multiple."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value:.1f}{suffix}"


def fmt_pct(value: Optional[float]) -> str:
    """Format a percentage value (stored as e.g. 25.0 = 25%)."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value:.1f}%"


def parse_date(value) -> Optional[date]:
    """
    Safely parse a date from a string, date, or None.
    Returns None on failure rather than raising.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(str(value)).date()
    except Exception:
        return None


def normalize_deal_type(raw: str) -> str:
    """Map raw deal type strings to canonical values."""
    mapping = {
        "lbo": "lbo",
        "leveraged buyout": "lbo",
        "strategic acquisition": "strategic_acquisition",
        "strategic_acquisition": "strategic_acquisition",
        "acquisition": "strategic_acquisition",
        "merger": "merger",
        "take private": "take_private",
        "take_private": "take_private",
        "carve out": "carve_out",
        "carve_out": "carve_out",
    }
    return mapping.get(str(raw).strip().lower(), str(raw).strip().lower())


def normalize_deal_status(raw: str) -> str:
    """Map raw deal status strings to canonical values."""
    mapping = {
        "closed": "closed",
        "completed": "closed",
        "announced": "announced",
        "pending": "pending",
        "terminated": "terminated",
        "withdrawn": "terminated",
    }
    return mapping.get(str(raw).strip().lower(), str(raw).strip().lower())


def quality_label(completeness_score: float) -> str:
    """Return a quality tier label given a completeness score (0–100)."""
    if completeness_score >= 80:
        return "High"
    elif completeness_score >= 50:
        return "Medium"
    return "Low"


def quality_color(label: str) -> str:
    """Return a hex color for a quality label (for Streamlit display)."""
    return {"High": "#00D4AA", "Medium": "#F7DC6F", "Low": "#FF6B6B"}.get(label, "#888888")
