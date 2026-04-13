"""
Sponsor Behavior Profiling, structured behavioral profiles for PE sponsors
with 3+ deals in the dataset.

Financial rationale: sponsor behavior is not random. Thoma Bravo systematically
targets application software. H&F concentrates in healthcare and financials.
Identifying a sponsor's valuation stance (premium vs value buyer), sector
concentration, and deal size preferences reveals persistent investment thesis
signals, useful for predicting where they will deploy next and how aggressively
they will compete in auction processes.
"""
import pandas as pd
import numpy as np
from ma.db import queries


def generate_sponsor_profile(sponsor_name: str, filters: dict = None) -> dict:
    """
    Generate a full behavioral profile for a single named sponsor.

    Returns a dict with:
        sponsor_name, deal_count, avg_ev_ebitda, market_median_ev_ebitda,
        ev_premium_vs_market, preferred_sectors, avg_deal_size_usd,
        market_avg_deal_size, deal_size_stance, deal_type_mix,
        valuation_stance, activity_regimes, narrative

    Requires at least 3 deals for the sponsor to generate a meaningful profile.
    Returns empty dict if insufficient data.
    """
    # Get sponsor deals
    sponsor_filters = dict(filters or {})
    sponsor_filters["acquirer_names"] = [sponsor_name]
    sponsor_filters["acquirer_types"] = ["sponsor"]
    df = queries.get_all_deals(sponsor_filters)

    if df.empty or len(df) < 3:
        return {}

    # Market-wide data for comparison
    market_df = queries.get_all_deals(filters)

    market_median_ev = float(market_df["ev_to_ebitda"].dropna().median()) if not market_df.empty else np.nan
    market_avg_deal_size = float(market_df["deal_value_usd"].dropna().mean()) if not market_df.empty else np.nan

    # Sponsor stats
    sponsor_ev = df["ev_to_ebitda"].dropna()
    ev_ebitda_count = int(sponsor_ev.count())
    avg_ev = float(sponsor_ev.mean()) if not sponsor_ev.empty else np.nan
    avg_deal_size = float(df["deal_value_usd"].dropna().mean()) if not df["deal_value_usd"].dropna().empty else np.nan

    # Preferred sectors (top 3 by deal count)
    preferred_sectors = []
    if "sector_name" in df.columns:
        sector_counts = df["sector_name"].dropna().value_counts()
        preferred_sectors = sector_counts.head(3).index.tolist()

    # Deal type mix
    deal_type_mix: dict = {}
    if "deal_type" in df.columns:
        type_counts = df["deal_type"].value_counts()
        total = type_counts.sum()
        deal_type_mix = {k: round(v / total * 100, 1) for k, v in type_counts.items()}

    # Valuation stance: premium buyer (>1x above market), value buyer (<-1x), market buyer
    # Requires at least 3 deals with valid EV/EBITDA for a meaningful classification
    ev_premium = avg_ev - market_median_ev if not np.isnan(avg_ev) and not np.isnan(market_median_ev) else np.nan
    MIN_SAMPLE = 3
    if ev_ebitda_count < MIN_SAMPLE:
        valuation_stance = "Insufficient valuation data"
    elif np.isnan(ev_premium):
        valuation_stance = "Unknown"
    elif ev_premium > 1.0:
        valuation_stance = "Premium Buyer"
    elif ev_premium < -1.0:
        valuation_stance = "Value Buyer"
    else:
        valuation_stance = "Market Buyer"

    # Deal size stance
    if np.isnan(avg_deal_size) or np.isnan(market_avg_deal_size):
        deal_size_stance = "Unknown"
    elif avg_deal_size > market_avg_deal_size * 1.2:
        deal_size_stance = "Large-Cap Focus"
    elif avg_deal_size < market_avg_deal_size * 0.8:
        deal_size_stance = "Mid-Market Focus"
    else:
        deal_size_stance = "Broadly Diversified"

    # Regime activity, which regimes is this sponsor most active in?
    activity_regimes: list = []
    if "announcement_year" in df.columns:
        try:
            from ma.analytics.regime import classify_regimes
            regime_df = classify_regimes(filters)
            if not regime_df.empty:
                sponsor_years = df["announcement_year"].dropna().astype(int).tolist()
                merged = regime_df[regime_df["year"].isin(sponsor_years)]
                if not merged.empty:
                    regime_counts = merged["regime_label"].value_counts()
                    activity_regimes = regime_counts.head(2).index.tolist()
        except Exception:
            pass

    profile = {
        "sponsor_name": sponsor_name,
        "deal_count": len(df),
        "ev_ebitda_count": ev_ebitda_count,
        "avg_ev_ebitda": round(avg_ev, 2) if not np.isnan(avg_ev) else None,
        "market_median_ev_ebitda": round(market_median_ev, 2) if not np.isnan(market_median_ev) else None,
        "ev_premium_vs_market": round(ev_premium, 2) if not np.isnan(ev_premium) else None,
        "preferred_sectors": preferred_sectors,
        "avg_deal_size_usd": round(avg_deal_size, 1) if not np.isnan(avg_deal_size) else None,
        "market_avg_deal_size": round(market_avg_deal_size, 1) if not np.isnan(market_avg_deal_size) else None,
        "deal_size_stance": deal_size_stance,
        "deal_type_mix": deal_type_mix,
        "valuation_stance": valuation_stance,
        "activity_regimes": activity_regimes,
    }
    profile["narrative"] = sponsor_profile_narrative(sponsor_name, filters, _profile=profile)
    return profile


def generate_all_profiles(filters: dict = None, min_deals: int = 3) -> pd.DataFrame:
    """
    Generate a summary profile table for all sponsors with at least min_deals deals.

    Returns a DataFrame with one row per sponsor:
        sponsor_name, deal_count, valuation_stance, avg_ev_ebitda,
        ev_premium_vs_market, preferred_sectors, deal_size_stance
    """
    # Get all sponsors with enough deals
    all_sponsors_df = queries.get_sponsor_rankings(filters, top_n=50)
    if all_sponsors_df.empty:
        return pd.DataFrame()

    eligible = all_sponsors_df[all_sponsors_df["deal_count"] >= min_deals]["sponsor_name"].tolist()
    if not eligible:
        return pd.DataFrame()

    rows = []
    for sponsor in eligible:
        profile = generate_sponsor_profile(sponsor, filters)
        if not profile:
            continue
        rows.append({
            "sponsor_name": profile["sponsor_name"],
            "deal_count": profile["deal_count"],
            "ev_ebitda_count": profile.get("ev_ebitda_count", 0),
            "valuation_stance": profile["valuation_stance"],
            "avg_ev_ebitda": profile.get("avg_ev_ebitda"),
            "ev_premium_vs_market": profile.get("ev_premium_vs_market"),
            "preferred_sectors": ", ".join(profile.get("preferred_sectors", [])[:2]),
            "deal_size_stance": profile.get("deal_size_stance"),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("deal_count", ascending=False).reset_index(drop=True)
    return df


def sponsor_profile_narrative(
    sponsor_name: str, filters: dict = None, _profile: dict = None
) -> str:
    """
    Returns analyst-style narrative for a single sponsor.

    E.g., 'Thoma Bravo is a premium buyer in Technology, systematically paying
    2.1x above market median EV/EBITDA. Active primarily during high-valuation
    regimes, with average deal size of $3.2B, targeting large-cap software assets
    where recurring revenue and switching costs justify elevated entry prices.'

    _profile: optionally pass a pre-computed profile dict to avoid re-fetching data.
    """
    profile = _profile or generate_sponsor_profile(sponsor_name, filters)
    if not profile:
        return f"Insufficient data to profile {sponsor_name} (fewer than 3 deals)."

    name = profile["sponsor_name"]
    stance = profile.get("valuation_stance", "Market Buyer")
    sectors = profile.get("preferred_sectors", [])
    sectors_str = ", ".join(sectors[:2]) if sectors else "multiple sectors"
    ev = profile.get("avg_ev_ebitda")
    premium = profile.get("ev_premium_vs_market")
    deal_size = profile.get("avg_deal_size_usd")
    size_stance = profile.get("deal_size_stance", "")
    deal_count = profile.get("deal_count", 0)
    regimes = profile.get("activity_regimes", [])
    deal_types = profile.get("deal_type_mix", {})

    ev_ebitda_count = profile.get("ev_ebitda_count", 0)

    # Valuation stance sentence
    if stance == "Insufficient valuation data":
        val_sentence = (
            f"**{name}** has {deal_count} deal(s) in the dataset. "
            f"Insufficient valuation data for classification "
            f"({ev_ebitda_count} deal(s) with valid EV/EBITDA, minimum 3 required)."
        )
    elif stance == "Premium Buyer" and premium is not None:
        val_sentence = (
            f"**{name}** is a **premium buyer**, paying an average EV/EBITDA of "
            f"**{ev:.1f}x**, {premium:+.1f}x above market median "
            f"(based on {ev_ebitda_count} deals with valuation data)."
        )
    elif stance == "Value Buyer" and premium is not None:
        val_sentence = (
            f"**{name}** is a **value-oriented buyer**, paying an average EV/EBITDA of "
            f"**{ev:.1f}x**, {premium:+.1f}x versus market median "
            f"(based on {ev_ebitda_count} deals with valuation data)."
        )
    elif ev:
        val_sentence = (
            f"**{name}** pays broadly in line with market, at an average EV/EBITDA of "
            f"**{ev:.1f}x** (based on {ev_ebitda_count} deals with valuation data)."
        )
    else:
        val_sentence = f"**{name}** has {deal_count} deals in the dataset."

    # Sector concentration sentence
    sector_sentence = (
        f"Sector concentration is highest in **{sectors_str}**."
        if sectors else ""
    )

    # Deal size sentence
    if deal_size and size_stance != "Unknown":
        size_sentence = (
            f"Average deal size of **${deal_size/1000:.1f}B** reflects a "
            f"**{size_stance.lower()}** mandate."
        )
    else:
        size_sentence = ""

    # Regime sentence
    regime_sentence = ""
    if regimes:
        regime_sentence = (
            f"Most active during **{regimes[0]}** market conditions"
            + (f" and **{regimes[1]}**" if len(regimes) > 1 else "")
            + " periods."
        )

    # Primary deal type
    type_sentence = ""
    if deal_types:
        primary_type = max(deal_types, key=deal_types.get)
        type_pct = deal_types[primary_type]
        type_sentence = f"Deal structure is predominantly **{primary_type.replace('_', ' ')}** ({type_pct:.0f}% of deals)."

    parts = [p for p in [val_sentence, sector_sentence, size_sentence, regime_sentence, type_sentence] if p]
    return " ".join(parts)
