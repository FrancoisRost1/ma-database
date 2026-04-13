"""
Market Regime Detection Engine, classifies annual M&A market periods based on
deal activity levels and valuation levels relative to full-period medians.

Financial rationale: market regimes capture the macro context in which deals
are struck. Entry timing matters, sponsors who deploy in trough/recovery
periods historically generate higher returns than those entering peak regimes.
Regime awareness also explains why valuation multiples and deal volumes move
together or diverge (liquidity conditions, credit availability, risk appetite).
"""
import pandas as pd
import numpy as np
from ma.db import queries


def classify_regimes(filters: dict = None) -> pd.DataFrame:
    """
    Classify each year in the dataset into a market regime based on
    deal activity and valuation levels relative to full-period medians.

    Returns a DataFrame with columns:
        year, deal_count, median_ev_ebitda, activity_level,
        valuation_level, sponsor_pct, regime_label

    Activity level: deal count vs full-period median.
    Valuation level: median EV/EBITDA vs full-period median.
    Sponsor dominance: sponsor % of total deals.
    """
    df = queries.get_all_deals(filters)
    if df.empty or "announcement_year" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["announcement_year"])
    if df.empty:
        return pd.DataFrame()

    # Annual aggregates
    annual_counts = (
        df.groupby("announcement_year")
        .agg(
            deal_count=("deal_id", "count"),
            median_ev_ebitda=("ev_to_ebitda", lambda x: x.dropna().median() if x.dropna().size > 0 else np.nan),
        )
        .reset_index()
        .rename(columns={"announcement_year": "year"})
    )

    # Sponsor % per year
    sponsor_df = df[df["acquirer_type"] == "sponsor"]
    sponsor_counts = sponsor_df.groupby("announcement_year").size().rename("sponsor_count").reset_index()
    sponsor_counts.columns = ["year", "sponsor_count"]
    annual_counts = annual_counts.merge(sponsor_counts, on="year", how="left")
    annual_counts["sponsor_count"] = annual_counts["sponsor_count"].fillna(0)
    annual_counts["sponsor_pct"] = (
        annual_counts["sponsor_count"] / annual_counts["deal_count"] * 100
    )

    # Full-period medians for thresholds
    activity_median = annual_counts["deal_count"].median()
    valuation_median = annual_counts["median_ev_ebitda"].dropna().median()

    def _activity_level(count: float) -> str:
        """Above full-period median = High Activity, below = Low Activity."""
        return "High Activity" if count >= activity_median else "Low Activity"

    def _valuation_level(ev: float) -> str:
        """Above full-period median EV/EBITDA = High Valuation, below = Low Valuation."""
        if pd.isna(ev):
            return "Unknown"
        return "High Valuation" if ev >= valuation_median else "Low Valuation"

    def _sponsor_dominance(pct: float) -> str:
        """
        > 45% sponsor share = Sponsor-Led market.
        < 35% = Strategic-Led.
        Otherwise Balanced.
        """
        if pct > 45:
            return "Sponsor-Led"
        if pct < 35:
            return "Strategic-Led"
        return "Balanced"

    def _regime_label(activity: str, valuation: str) -> str:
        """
        Four-quadrant regime classification:
        - High Activity + High Valuation  → Peak / Late-Cycle
        - High Activity + Low Valuation   → Recovery / Opportunity
        - Low Activity + High Valuation   → Selective / Cautious
        - Low Activity + Low Valuation    → Trough / Distressed
        """
        if activity == "High Activity" and valuation == "High Valuation":
            return "Peak / Late-Cycle"
        if activity == "High Activity" and valuation == "Low Valuation":
            return "Recovery / Opportunity"
        if activity == "Low Activity" and valuation == "High Valuation":
            return "Selective / Cautious"
        if activity == "Low Activity" and valuation == "Low Valuation":
            return "Trough / Distressed"
        return "Indeterminate"

    annual_counts["activity_level"] = annual_counts["deal_count"].apply(_activity_level)
    annual_counts["valuation_level"] = annual_counts["median_ev_ebitda"].apply(_valuation_level)
    annual_counts["sponsor_dominance"] = annual_counts["sponsor_pct"].apply(_sponsor_dominance)
    annual_counts["regime_label"] = annual_counts.apply(
        lambda r: _regime_label(r["activity_level"], r["valuation_level"]), axis=1
    )

    return annual_counts.sort_values("year").reset_index(drop=True)


def get_current_regime(filters: dict = None) -> dict:
    """
    Returns the most recent year's regime classification with explanation text.
    Used in the Overview tab regime callout and snapshot memo.

    Returns dict with: year, regime_label, activity_level, valuation_level,
    sponsor_pct, sponsor_dominance, median_ev_ebitda, deal_count, explanation.
    """
    regime_df = classify_regimes(filters)
    if regime_df.empty:
        return {}

    latest = regime_df.iloc[-1].to_dict()
    label = latest.get("regime_label", "Indeterminate")
    year = int(latest.get("year", 0))
    ev = latest.get("median_ev_ebitda")
    sp_pct = latest.get("sponsor_pct", 0)

    # Build explanation text
    _explanations = {
        "Peak / Late-Cycle": (
            f"High deal activity and elevated multiples ({ev:.1f}x median EV/EBITDA) "
            f"are characteristic of late-cycle conditions. Sponsor entry risk increases "
            f"as leverage multiples compress returns and exit optionality narrows."
        ) if ev else (
            "High deal activity with elevated valuations, characteristic of late-cycle conditions."
        ),
        "Recovery / Opportunity": (
            f"Rising deal activity with contained multiples ({ev:.1f}x median EV/EBITDA) "
            f"signals a recovery window. Credit availability is improving but competition "
            f"has not yet driven up entry prices, historically the best entry vintage."
        ) if ev else (
            "Rising activity with below-median valuations, a recovery entry window."
        ),
        "Selective / Cautious": (
            f"Low deal volume despite elevated multiples ({ev:.1f}x) suggests buyers are "
            f"being selective, only high-conviction deals pencil at these prices. "
            f"Multiple compression risk is elevated for new entrants."
        ) if ev else (
            "Low volume with high multiples, selective market, multiple compression risk."
        ),
        "Trough / Distressed": (
            f"Depressed activity and below-average multiples ({ev:.1f}x) point to a trough. "
            f"Dry powder deployment is constrained by credit conditions or macro uncertainty. "
            f"Historically precedes strong vintage returns for patient capital."
        ) if ev else (
            "Low activity and low multiples, trough/distressed conditions."
        ),
        "Indeterminate": "Insufficient data to classify regime with confidence.",
    }

    latest["explanation"] = _explanations.get(label, "")
    return latest


def regime_transition_summary(filters: dict = None) -> str:
    """
    Returns analyst-style narrative describing how the market regime has evolved.
    E.g., 'The market transitioned from Peak/Late-Cycle in 2021 to
    Selective/Cautious in 2022, driven by rising rates compressing deal activity
    while multiples remained elevated.'

    Returns empty string if insufficient data.
    """
    regime_df = classify_regimes(filters)
    if regime_df.empty or len(regime_df) < 2:
        return ""

    # Find transitions, years where regime label changes
    transitions = []
    for i in range(1, len(regime_df)):
        prev = regime_df.iloc[i - 1]
        curr = regime_df.iloc[i]
        if prev["regime_label"] != curr["regime_label"]:
            transitions.append({
                "from_year": int(prev["year"]),
                "to_year": int(curr["year"]),
                "from_label": prev["regime_label"],
                "to_label": curr["regime_label"],
            })

    if not transitions:
        latest = regime_df.iloc[-1]
        return (
            f"The M&A market has remained in a **{latest['regime_label']}** regime "
            f"throughout the selected period, with deal count and valuation levels "
            f"consistently tracking {'above' if latest['activity_level'] == 'High Activity' else 'below'} "
            f"their long-run medians."
        )

    # Summarize last 2 transitions max
    last_transitions = transitions[-2:]
    parts = []
    for t in last_transitions:
        parts.append(
            f"**{t['from_label']}** ({t['from_year']}) → **{t['to_label']}** ({t['to_year']})"
        )

    latest = regime_df.iloc[-1]
    summary = (
        f"The market transitioned through {' and '.join(parts)}. "
        f"The current regime ({int(latest['year'])}) is classified as "
        f"**{latest['regime_label']}**, "
        f"{latest['activity_level'].lower()} with {latest['valuation_level'].lower()}."
    )
    return summary
