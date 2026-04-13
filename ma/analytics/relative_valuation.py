"""
Relative Valuation Analysis, sector premium/discount vs market, historical
percentile positioning, and sponsor vs strategic spread by sector.

Financial rationale: absolute EV/EBITDA multiples are less informative than
relative metrics. A sector at 15x is cheap if it normally trades at 18x (17th
percentile) and expensive if it normally trades at 11x (95th percentile).
Relative valuation analysis replicates how M&A desks benchmark entry prices
and defend them to investment committees.
"""
import pandas as pd
import numpy as np
from ma.db import queries


def sector_relative_valuation(filters: dict = None) -> pd.DataFrame:
    """
    Per-sector median EV/EBITDA vs market-wide median, plus historical percentile.

    Historical percentile: where does the current sector median sit within
    the sector's own full-history distribution? Computed over all years in
    the dataset (not just the filtered period).

    Returns DataFrame with:
        sector_name, sector_median, market_median, premium_discount,
        historical_percentile, interpretation_label
    """
    df = queries.get_all_deals(filters)
    if df.empty or "ev_to_ebitda" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["ev_to_ebitda", "sector_name"])
    if df.empty:
        return pd.DataFrame()

    # Market-wide median across all deals in filtered set
    market_median = float(df["ev_to_ebitda"].median())

    # Per-sector median
    sector_stats = (
        df.groupby("sector_name")["ev_to_ebitda"]
        .agg(
            deal_count="count",
            sector_median=lambda x: float(x.median()),
        )
        .reset_index()
    )
    sector_stats = sector_stats[sector_stats["deal_count"] >= 3]

    # Historical percentile: full-history data (no date filter) per sector
    full_filters = {k: v for k, v in (filters or {}).items()
                   if k not in ("year_start", "year_end")}
    full_df = queries.get_all_deals(full_filters)
    full_df = full_df.dropna(subset=["ev_to_ebitda", "sector_name"])

    def _historical_percentile(sector: str, current_median: float) -> float:
        """
        Percentile rank of current_median within the sector's own historical distribution.
        E.g., 78th percentile means current multiple is higher than 78% of all
        historical observations for that sector.
        """
        hist = full_df[full_df["sector_name"] == sector]["ev_to_ebitda"].dropna()
        if hist.empty:
            return np.nan
        return float((hist < current_median).mean() * 100)

    sector_stats["market_median"] = market_median
    sector_stats["premium_discount"] = sector_stats["sector_median"] - market_median

    sector_stats["historical_percentile"] = sector_stats.apply(
        lambda r: _historical_percentile(r["sector_name"], r["sector_median"]), axis=1
    )

    def _interpretation_label(pct: float) -> str:
        if pd.isna(pct):
            return "N/A"
        if pct >= 80:
            return "Elevated (top quintile)"
        if pct >= 60:
            return "Above average"
        if pct >= 40:
            return "Near historical average"
        if pct >= 20:
            return "Below average"
        return "Depressed (bottom quintile)"

    sector_stats["interpretation_label"] = sector_stats["historical_percentile"].apply(_interpretation_label)

    return (
        sector_stats
        .sort_values("premium_discount", ascending=False)
        .reset_index(drop=True)
    )


def sector_premium_trend(filters: dict = None) -> pd.DataFrame:
    """
    How each sector's premium/discount vs the market-wide median evolves over time.

    Computes market median per year, then sector median per year, then the spread.
    Returns: year, sector_name, sector_median, market_median, premium_discount

    Useful for detecting valuation regime shifts, e.g., Tech premium expanding
    during software boom, then compressing post-rate hike.
    """
    df = queries.get_all_deals(filters)
    if df.empty or "ev_to_ebitda" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["ev_to_ebitda", "sector_name", "announcement_year"])
    if df.empty:
        return pd.DataFrame()

    # Market median per year
    market_by_year = (
        df.groupby("announcement_year")["ev_to_ebitda"]
        .median()
        .rename("market_median")
        .reset_index()
        .rename(columns={"announcement_year": "year"})
    )

    # Sector median per year (only sectors with 2+ deals in that year)
    sector_by_year = (
        df.groupby(["announcement_year", "sector_name"])
        .agg(
            sector_median=("ev_to_ebitda", "median"),
            deal_count=("ev_to_ebitda", "count"),
        )
        .reset_index()
        .rename(columns={"announcement_year": "year"})
    )
    sector_by_year = sector_by_year[sector_by_year["deal_count"] >= 2]

    merged = sector_by_year.merge(market_by_year, on="year", how="left")
    merged["premium_discount"] = merged["sector_median"] - merged["market_median"]

    return merged.sort_values(["sector_name", "year"]).reset_index(drop=True)


def sponsor_vs_strategic_premium(filters: dict = None) -> pd.DataFrame:
    """
    Per-sector comparison of sponsor median EV/EBITDA vs strategic median EV/EBITDA.
    Spread = sponsor_median - strategic_median.
    Positive spread = sponsors outbid strategics in this sector.

    Financial rationale: where sponsors pay more than strategics, they believe
    leveraged returns justify higher entry; where strategics pay more, synergy
    pricing dominates (they can afford higher prices without leverage dependency).

    Returns: sector_name, sponsor_median, strategic_median, spread, interpretation
    """
    df = queries.get_all_deals(filters)
    if df.empty or "ev_to_ebitda" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["ev_to_ebitda", "sector_name", "acquirer_type"])
    df = df[df["acquirer_type"].isin(["sponsor", "strategic"])]
    if df.empty:
        return pd.DataFrame()

    pivot = (
        df.groupby(["sector_name", "acquirer_type"])["ev_to_ebitda"]
        .agg(median="median", count="count")
        .reset_index()
    )

    # Need both sponsor and strategic with >=3 deals each per sector
    sponsors = pivot[(pivot["acquirer_type"] == "sponsor") & (pivot["count"] >= 3)][
        ["sector_name", "median", "count"]
    ].rename(columns={"median": "sponsor_median", "count": "sponsor_count"})

    strategics = pivot[(pivot["acquirer_type"] == "strategic") & (pivot["count"] >= 3)][
        ["sector_name", "median", "count"]
    ].rename(columns={"median": "strategic_median", "count": "strategic_count"})

    merged = sponsors.merge(strategics, on="sector_name", how="inner")
    if merged.empty:
        return pd.DataFrame()

    merged["spread"] = merged["sponsor_median"] - merged["strategic_median"]
    merged["sponsor_median"] = merged["sponsor_median"].round(2)
    merged["strategic_median"] = merged["strategic_median"].round(2)
    merged["spread"] = merged["spread"].round(2)

    def _interpretation(row) -> str:
        spread = row["spread"]
        sector = row["sector_name"]
        if spread > 1.5:
            return (
                f"Sponsors pay meaningfully more in {sector} (+{spread:.1f}x), "
                f"suggesting leveraged return profiles justify premium entry."
            )
        if spread < -1.5:
            return (
                f"Strategics outbid sponsors in {sector} by {abs(spread):.1f}x, "
                f"reflecting synergy-driven pricing that sponsors cannot match."
            )
        return f"Sponsor and strategic pricing are broadly aligned in {sector} ({spread:+.1f}x spread)."

    merged["interpretation"] = merged.apply(_interpretation, axis=1)
    return merged.sort_values("spread", ascending=False).reset_index(drop=True)


def relative_valuation_narrative(filters: dict = None) -> str:
    """
    Analyst-style narrative summarizing relative valuation landscape.

    E.g., 'Technology trades at a +2.3x premium to the broader market, at the
    78th percentile of its 10-year range. Sponsors pay 1.1x more than strategics
    in this sector, consistent with competitive software buyout dynamics.
    Energy trades at a -2.8x discount, at the 24th historical percentile,
    suggesting relative value versus the broader M&A market.'
    """
    rel_df = sector_relative_valuation(filters)
    sv_df = sponsor_vs_strategic_premium(filters)

    if rel_df.empty:
        return "Insufficient valuation data for relative analysis."

    parts = []

    # Top premium and discount sectors
    top_premium = rel_df[rel_df["premium_discount"] > 0].head(2)
    top_discount = rel_df[rel_df["premium_discount"] < 0].tail(2)

    for _, row in top_premium.iterrows():
        pct = row.get("historical_percentile")
        pct_str = f", at the **{pct:.0f}th percentile** of its historical range" if not pd.isna(pct) else ""
        parts.append(
            f"**{row['sector_name']}** trades at a **+{row['premium_discount']:.1f}x premium** "
            f"to the market ({row['sector_median']:.1f}x median){pct_str}."
        )

    for _, row in top_discount.iterrows():
        pct = row.get("historical_percentile")
        pct_str = f", at the **{pct:.0f}th percentile** of its historical range" if not pd.isna(pct) else ""
        parts.append(
            f"**{row['sector_name']}** trades at a **{row['premium_discount']:.1f}x discount** "
            f"to market ({row['sector_median']:.1f}x){pct_str}."
        )

    # Top sponsor/strategic spread sector
    if not sv_df.empty:
        top_spread = sv_df.iloc[0]
        if abs(top_spread["spread"]) > 0.5:
            direction = "above" if top_spread["spread"] > 0 else "below"
            parts.append(
                f"In **{top_spread['sector_name']}**, sponsors pay **{abs(top_spread['spread']):.1f}x {direction}** "
                f"strategic acquirers ({top_spread['sponsor_median']:.1f}x vs {top_spread['strategic_median']:.1f}x)."
            )

    return " ".join(parts) if parts else "Insufficient data for relative valuation narrative."
