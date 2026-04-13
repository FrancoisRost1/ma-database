"""
Market Imbalance Detection, identifies sectors showing overheating, cooling,
or healthy growth signals based on activity momentum and valuation momentum.

Financial rationale: M&A cycles are non-linear. A sector can go from underserved
to overbid within 18 months as dry powder concentrates and competition intensifies.
Early detection of overheating (rising activity + rising multiples) helps
investors assess auction dynamics before entering a process. Detection of cooling
sectors (falling activity + falling multiples) surfaces potential value windows
before the crowd arrives.

Momentum windows: last 2 years vs prior 3 years. Requires at least 5 years of
data per sector for meaningful signal.
"""
import pandas as pd
import numpy as np
from ma.db import queries


MIN_SAMPLE = 3  # Minimum deal count for a reliable signal


def signal_confidence(n: int) -> str:
    """
    Return confidence qualifier based on deal count backing a signal.
    n >= 10 → high confidence
    n  5-9  → moderate confidence
    n  3-4  → low confidence
    n < 3   → insufficient data
    """
    if n >= 10:
        return "high confidence"
    if n >= 5:
        return "moderate confidence"
    if n >= MIN_SAMPLE:
        return "low confidence"
    return "insufficient data"


def detect_sector_imbalances(filters: dict = None) -> pd.DataFrame:
    """
    Compute activity momentum and valuation momentum per sector, then classify
    into one of four imbalance signals:
        - Overheating: activity accelerating + valuation rising
        - Healthy Growth: activity accelerating + valuation flat/falling
        - Narrowing: activity declining + valuation rising
        - Cooling: activity declining + valuation falling

    Momentum = % change from prior 3-year avg to last 2-year avg.
    Requires a sector to have data in both windows (prior and recent).

    Returns DataFrame:
        sector_name, activity_momentum_pct, valuation_momentum_pct,
        recent_deal_count, prior_deal_count, recent_ev_median, prior_ev_median,
        signal, interpretation
    """
    df = queries.get_all_deals(filters)
    if df.empty or "announcement_year" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["announcement_year", "sector_name"])
    if df.empty:
        return pd.DataFrame()

    all_years = sorted(df["announcement_year"].dropna().astype(int).unique())
    if len(all_years) < 5:
        # Insufficient history for momentum analysis
        return pd.DataFrame()

    # Define windows: last 2 years = recent, 3 years before that = prior
    recent_years = all_years[-2:]
    prior_years = all_years[-5:-2]

    recent_df = df[df["announcement_year"].isin(recent_years)]
    prior_df = df[df["announcement_year"].isin(prior_years)]

    # Per-sector aggregates in each window
    def _agg(window_df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        agg = (
            window_df.groupby("sector_name")
            .agg(
                deal_count=("deal_id", "count"),
                ev_median=("ev_to_ebitda", lambda x: x.dropna().median() if x.dropna().size > 0 else np.nan),
            )
            .reset_index()
        )
        agg.columns = ["sector_name", f"{prefix}_deal_count", f"{prefix}_ev_median"]
        return agg

    recent_agg = _agg(recent_df, "recent")
    prior_agg = _agg(prior_df, "prior")

    # Normalize prior counts for period length difference (2 years vs 3 years)
    # Compare annualized rates to avoid penalizing shorter recent window
    prior_agg["prior_deal_count_annual"] = prior_agg["prior_deal_count"] / max(len(prior_years), 1)
    recent_agg["recent_deal_count_annual"] = recent_agg["recent_deal_count"] / max(len(recent_years), 1)

    merged = recent_agg.merge(prior_agg, on="sector_name", how="inner")
    if merged.empty:
        return pd.DataFrame()

    # Momentum calculations
    def _safe_pct_change(recent: float, prior: float) -> float:
        if pd.isna(recent) or pd.isna(prior) or prior == 0:
            return np.nan
        return ((recent - prior) / prior) * 100

    merged["activity_momentum_pct"] = merged.apply(
        lambda r: _safe_pct_change(r["recent_deal_count_annual"], r["prior_deal_count_annual"]),
        axis=1,
    )
    merged["valuation_momentum_pct"] = merged.apply(
        lambda r: _safe_pct_change(r["recent_ev_median"], r["prior_ev_median"]),
        axis=1,
    )

    # Signal classification
    # Threshold: >10% change = meaningful movement; <-10% = meaningful decline
    ACTIVITY_THRESHOLD = 10.0
    VALUATION_THRESHOLD = 5.0

    def _classify_signal(act: float, val: float) -> str:
        """
        Four-quadrant classification based on activity and valuation momentum.
        Overheating: both rising, competitive pressure driving prices above fundamentals.
        Healthy Growth: rising activity, flat/falling multiples, demand not yet reflected in price.
        Narrowing: falling activity but rising multiples, only select deals clear the bar.
        Cooling: both falling, disengagement, potential value emerging.
        """
        if pd.isna(act) or pd.isna(val):
            return "Insufficient Data"
        act_up = act > ACTIVITY_THRESHOLD
        val_up = val > VALUATION_THRESHOLD
        if act_up and val_up:
            return "Overheating"
        if act_up and not val_up:
            return "Healthy Growth"
        if not act_up and val_up:
            return "Narrowing"
        return "Cooling"

    merged["total_deal_count"] = merged["recent_deal_count"] + merged["prior_deal_count"]

    merged["signal"] = merged.apply(
        lambda r: _classify_signal(r["activity_momentum_pct"], r["valuation_momentum_pct"]),
        axis=1,
    )

    def _interpretation(row) -> str:
        signal = row["signal"]
        sector = row["sector_name"]
        act = row["activity_momentum_pct"]
        val = row["valuation_momentum_pct"]
        ev = row.get("recent_ev_median")

        ev_str = f" ({ev:.1f}x median EV/EBITDA)" if not pd.isna(ev) else ""

        if signal == "Overheating":
            return (
                f"{sector}: deal volume up {act:.0f}% with multiples expanding "
                f"{val:+.0f}%{ev_str}. Competitive pressure is driving valuations "
                f"above fundamental support, new entrants face elevated entry risk."
            )
        if signal == "Healthy Growth":
            return (
                f"{sector}: rising deal activity ({act:+.0f}%) with contained multiples "
                f"({val:+.0f}%){ev_str}. Demand is increasing but not yet reflected in "
                f"price, a constructive entry environment."
            )
        if signal == "Narrowing":
            return (
                f"{sector}: declining activity ({act:.0f}%) while multiples expand "
                f"({val:+.0f}%){ev_str}. Only high-conviction deals are clearing at "
                f"elevated prices, selection quality risk is rising."
            )
        if signal == "Cooling":
            return (
                f"{sector}: both deal volume ({act:.0f}%) and multiples ({val:.0f}%) "
                f"are contracting{ev_str}. Disengagement may be creating a relative "
                f"value window for patient capital."
            )
        return f"{sector}: insufficient data to determine market signal."

    merged["interpretation"] = merged.apply(_interpretation, axis=1)

    # Clean up output columns
    output = merged[[
        "sector_name", "activity_momentum_pct", "valuation_momentum_pct",
        "recent_deal_count", "prior_deal_count", "total_deal_count",
        "recent_ev_median", "prior_ev_median",
        "signal", "interpretation",
    ]].copy()

    for col in ["activity_momentum_pct", "valuation_momentum_pct"]:
        output[col] = output[col].round(1)
    for col in ["recent_ev_median", "prior_ev_median"]:
        output[col] = output[col].round(2)

    return output.sort_values("signal").reset_index(drop=True)


def market_heat_map(filters: dict = None) -> pd.DataFrame:
    """
    Returns data for a 2×2 scatter plot:
        x = activity_momentum_pct (deal volume change)
        y = valuation_momentum_pct (EV/EBITDA change)
        color = signal
        label = sector_name

    Intended for a Plotly scatter chart with quadrant shading.
    """
    df = detect_sector_imbalances(filters)
    if df.empty:
        return pd.DataFrame()

    return df[["sector_name", "activity_momentum_pct", "valuation_momentum_pct",
               "signal", "recent_ev_median"]].dropna(
        subset=["activity_momentum_pct", "valuation_momentum_pct"]
    )


def imbalance_narrative(filters: dict = None) -> str:
    """
    Analyst-style narrative summarizing the top 2-3 sector signals.

    E.g., 'Technology shows Overheating signals, deal activity up 35% while
    multiples expanded 2.1x. Healthcare shows Healthy Growth, rising activity
    with stable multiples, suggesting a constructive entry window.'
    """
    df = detect_sector_imbalances(filters)
    if df.empty:
        return "Insufficient history for sector momentum analysis (requires 5+ years of data)."

    # Prioritize Overheating and Cooling for narrative (most actionable signals)
    priority_order = ["Overheating", "Cooling", "Narrowing", "Healthy Growth"]
    parts = []
    seen_signals = set()

    for signal in priority_order:
        rows = df[df["signal"] == signal]
        if rows.empty:
            continue
        for _, row in rows.head(2).iterrows():
            parts.append(f"- **{row['sector_name']}** ({row['signal']}): {row['interpretation']}")
            seen_signals.add(signal)
        if len(parts) >= 4:
            break

    # Fill remaining with other signals if < 2 parts
    if len(parts) < 2:
        for _, row in df.iterrows():
            if row["signal"] not in seen_signals:
                parts.append(f"- **{row['sector_name']}** ({row['signal']}): {row['interpretation']}")
            if len(parts) >= 2:
                break

    if not parts:
        return "No actionable sector imbalance signals detected in the current period."

    return "\n".join(parts)
