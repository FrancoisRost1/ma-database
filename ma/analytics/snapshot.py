"""
M&A Market Snapshot — strategic analyst-style commentary memo.
Upgraded from descriptive ("what happened") to strategic ("what it means
and what to watch").

Sections:
1. Market Regime — current regime + recent transition
2. Sector Signals — top sectors with imbalance signals and relative valuation
3. Sponsor Activity — top sponsors with behavioral context
4. Valuation Environment — overall market valuation, sponsor vs strategic spread
5. Watch List — 2-3 things to monitor going forward

Max ~400 words per config.yaml snapshot.max_length_words.
"""
import pandas as pd
from ma.db import queries
from ma.analytics import market_activity, valuation, sponsor_intel
from ma.analytics import regime as regime_mod
from ma.analytics import relative_valuation as rel_val_mod
from ma.analytics import imbalance as imbalance_mod
from ma.analytics import sponsor_profile as sponsor_profile_mod
from ma.analytics import interpretation as interp_mod


def generate_snapshot(filters: dict = None, config: dict = None) -> str:
    """
    Generate a strategic markdown-formatted M&A Market Snapshot memo.
    Incorporates regime context, relative valuation, imbalance signals,
    and sponsor behavioral profiles. Every observation ends with
    'which suggests...' or 'investors should watch...' framing.
    """
    kpis = queries.get_kpi_summary(filters)
    if not kpis or kpis.get("total_deals", 0) == 0:
        return "_No deals found for the selected filters. Adjust the sidebar to broaden the scope._"

    total = kpis.get("total_deals", 0)
    total_value = kpis.get("total_deal_value_usd", 0) or 0
    most_active_sector = kpis.get("most_active_sector", "N/A")
    most_active_sponsor = kpis.get("most_active_sponsor", "N/A")
    sponsor_count = kpis.get("sponsor_deal_count", 0)
    sponsor_pct = (sponsor_count / total * 100) if total > 0 else 0

    # Year range
    df_all = queries.get_all_deals(filters)
    year_range_str = "N/A"
    if not df_all.empty and "announcement_year" in df_all.columns:
        years = df_all["announcement_year"].dropna()
        if not years.empty:
            year_range_str = f"{int(years.min())}–{int(years.max())}"

    lines = [f"### M&A Market Snapshot — {year_range_str}", ""]

    # -------------------------------------------------------------------
    # SECTION 1: Market Regime
    # -------------------------------------------------------------------
    lines.append("**Market Regime**")
    current_regime = regime_mod.get_current_regime(filters)
    transition_text = regime_mod.regime_transition_summary(filters)

    if current_regime:
        label = current_regime.get("regime_label", "Indeterminate")
        year = current_regime.get("year")
        explanation = current_regime.get("explanation", "")
        regime_interp = interp_mod.interpret_regime(label)

        regime_line = f"Current regime ({year}): **{label}**."
        if explanation:
            regime_line += f" {explanation}"
        lines.append(regime_line)
        if transition_text:
            lines.append(transition_text)
    else:
        lines.append("Insufficient data to classify market regime.")
    lines.append("")

    # -------------------------------------------------------------------
    # SECTION 2: Sector Signals (imbalance + relative valuation)
    # -------------------------------------------------------------------
    lines.append("**Sector Signals**")

    imbalance_df = imbalance_mod.detect_sector_imbalances(filters)
    rel_df = rel_val_mod.sector_relative_valuation(filters)

    if not imbalance_df.empty:
        # Lead with the most actionable signals: Overheating and Cooling
        for signal in ["Overheating", "Cooling"]:
            signal_rows = imbalance_df[imbalance_df["signal"] == signal]
            for _, row in signal_rows.head(1).iterrows():
                sector = row["sector_name"]
                act = row["activity_momentum_pct"]
                val_mom = row["valuation_momentum_pct"]
                ev_now = row.get("recent_ev_median")

                # Enrich with relative valuation percentile if available
                pct_str = ""
                if not rel_df.empty:
                    rel_row = rel_df[rel_df["sector_name"] == sector]
                    if not rel_row.empty:
                        pct = rel_row.iloc[0].get("historical_percentile")
                        if pd.notna(pct):
                            pct_str = f" ({pct:.0f}th percentile of historical range)"

                ev_str = f" at {ev_now:.1f}x EV/EBITDA{pct_str}" if ev_now and not pd.isna(ev_now) else pct_str

                if signal == "Overheating":
                    lines.append(
                        f"**{sector}** shows late-cycle characteristics — activity up {act:.0f}% "
                        f"with multiples expanding {val_mom:+.0f}%{ev_str}. "
                        f"Investors entering auction processes here should stress-test under "
                        f"multiple compression scenarios."
                    )
                else:  # Cooling
                    lines.append(
                        f"**{sector}** is cooling — activity {act:.0f}% and multiples "
                        f"contracting{ev_str}. "
                        f"Potential relative value window; watch for motivated secondary sellers."
                    )

        # Add one Healthy Growth sector as a constructive signal
        healthy_rows = imbalance_df[imbalance_df["signal"] == "Healthy Growth"]
        for _, row in healthy_rows.head(1).iterrows():
            sector = row["sector_name"]
            act = row["activity_momentum_pct"]
            lines.append(
                f"**{sector}** presents a constructive entry profile — rising activity "
                f"({act:+.0f}%) with contained multiples, which suggests a healthy demand/price dynamic."
            )
    elif not rel_df.empty:
        # Fall back to relative valuation narrative
        lines.append(rel_val_mod.relative_valuation_narrative(filters))

    if imbalance_df.empty and rel_df.empty:
        lines.append(f"Most active sector: **{most_active_sector}**.")
    lines.append("")

    # -------------------------------------------------------------------
    # SECTION 3: Sponsor Activity
    # -------------------------------------------------------------------
    lines.append("**Sponsor Activity**")

    sponsor_df = sponsor_intel.sponsor_rankings(filters, top_n=3)
    top_sponsors = sponsor_df["sponsor_name"].tolist() if not sponsor_df.empty else []

    if top_sponsors:
        # Profile the top sponsor
        top_sponsor = top_sponsors[0]
        profile = sponsor_profile_mod.generate_sponsor_profile(top_sponsor, filters)

        if profile:
            stance = profile.get("valuation_stance", "Market Buyer")
            sectors_str = ", ".join(profile.get("preferred_sectors", [])[:2])
            ev = profile.get("avg_ev_ebitda")
            premium = profile.get("ev_premium_vs_market")

            if premium is not None and abs(premium) > 0.3:
                premium_str = f" — paying {premium:+.1f}x {'above' if premium > 0 else 'below'} market median"
            else:
                premium_str = ""

            lines.append(
                f"**{top_sponsor}** leads PE deal flow and is classified as a "
                f"**{stance}**{premium_str}, concentrating in {sectors_str if sectors_str else 'multiple sectors'}. "
                + (f"Average entry EV/EBITDA: **{ev:.1f}x**. " if ev else "")
                + (f"Other active sponsors: {', '.join(top_sponsors[1:])}. " if len(top_sponsors) > 1 else "")
                + f"Sponsor activity accounts for **{sponsor_pct:.0f}%** of deal count."
            )
        else:
            sponsors_str = ", ".join(top_sponsors)
            lines.append(
                f"**{sponsors_str}** led PE deal flow. "
                f"Sponsor activity accounts for **{sponsor_pct:.0f}%** of deal count, "
                f"which suggests {'sponsor-dominated' if sponsor_pct > 45 else 'balanced'} deal flow."
            )
    else:
        lines.append("No sponsor deal flow in the selected period.")
    lines.append("")

    # -------------------------------------------------------------------
    # SECTION 4: Valuation Environment
    # -------------------------------------------------------------------
    lines.append("**Valuation Environment**")

    val_regime = valuation.valuation_regime_comparison(filters)
    current_median = val_regime.get("current_median")
    prior_median = val_regime.get("prior_median")
    direction = val_regime.get("direction", "stable")
    change_pct = val_regime.get("change_pct")

    spvs_df = valuation.sponsor_vs_strategic_multiples(filters)
    sponsor_median_ev = None
    strategic_median_ev = None
    if not spvs_df.empty:
        sp_row = spvs_df[spvs_df["acquirer_type"] == "sponsor"]
        st_row = spvs_df[spvs_df["acquirer_type"] == "strategic"]
        if not sp_row.empty:
            sponsor_median_ev = sp_row.iloc[0]["median_ev_to_ebitda"]
        if not st_row.empty:
            strategic_median_ev = st_row.iloc[0]["median_ev_to_ebitda"]

    if current_median:
        dir_label = {"expansion": "expanded", "compression": "compressed", "stable": "remained stable"}.get(direction, "shifted")
        val_line = f"Market-wide median EV/EBITDA: **{current_median:.1f}x**"
        if prior_median and change_pct is not None:
            val_line += f", having {dir_label} from {prior_median:.1f}x ({change_pct:+.1f}%)"
        val_line += "."

        # Relative valuation context for market overall
        if direction == "expansion":
            val_line += " Investors should watch for earnings visibility risk if multiples outpace fundamentals."
        elif direction == "compression":
            val_line += " Multiple compression may be creating selective entry opportunities for patient capital."
        lines.append(val_line)

    if sponsor_median_ev and strategic_median_ev:
        spread = sponsor_median_ev - strategic_median_ev
        spread_interp = interp_mod.interpret_sponsor_vs_strategic_spread(most_active_sector, spread)
        lines.append(
            f"Sponsor vs. strategic entry multiples: **{sponsor_median_ev:.1f}x** vs "
            f"**{strategic_median_ev:.1f}x** ({spread:+.1f}x spread). {spread_interp}"
        )
    lines.append("")

    # -------------------------------------------------------------------
    # SECTION 5: Watch List
    # -------------------------------------------------------------------
    lines.append("**Watch List**")

    watch_items = []

    # Regime-driven watch item
    if current_regime:
        r_label = current_regime.get("regime_label", "")
        if r_label == "Peak / Late-Cycle":
            watch_items.append("Monitor for multiple compression as credit conditions evolve — exits may face narrower spread vs entry.")
        elif r_label == "Trough / Distressed":
            watch_items.append("Track credit market reopening signals — recovery vintages deploy when deal flow is still thin.")
        elif r_label == "Recovery / Opportunity":
            watch_items.append("Window may be closing — watch for deal count acceleration as a leading indicator that competition is intensifying.")

    # Overheating sector watch
    if not imbalance_df.empty:
        overheating = imbalance_df[imbalance_df["signal"] == "Overheating"]["sector_name"].head(1).tolist()
        cooling = imbalance_df[imbalance_df["signal"] == "Cooling"]["sector_name"].head(1).tolist()
        if overheating:
            watch_items.append(f"**{overheating[0]}** — overheating signals warrant monitoring for auction discipline and leverage underwriting standards.")
        if cooling:
            watch_items.append(f"**{cooling[0]}** — cooling activity may surface secondary deal flow at improving prices.")

    # Default watch items if none generated
    if not watch_items:
        watch_items.append(f"**{most_active_sector}** deal flow concentration — monitor for sector rotation signals.")
        watch_items.append("Sponsor dry powder deployment pace relative to credit availability.")

    for item in watch_items[:3]:
        lines.append(f"- {item}")
    lines.append("")

    # Data note
    real_count = kpis.get("real_deals", 0)
    synth_count = kpis.get("synthetic_deals", 0)
    lines.append(
        f"_Data: {real_count} verified real transactions + {synth_count} synthetic records. "
        f"Total disclosed value: **${total_value/1000:.1f}B**. "
        f"Use the sidebar **Data Origin** filter to view real deals only._"
    )

    return "\n".join(lines)
