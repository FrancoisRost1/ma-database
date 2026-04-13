"""
M&A Market Snapshot, strategic analyst-style commentary memo.
Upgraded from descriptive ("what happened") to strategic ("what it means
and what to watch").

Sections:
1. Market Regime, current regime + recent transition
2. Sector Signals, top sectors with imbalance signals and relative valuation
3. Sponsor Activity, top sponsors with behavioral context
4. Valuation Environment, overall market valuation, sponsor vs strategic spread
5. Watch List, 2-3 things to monitor going forward

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

MIN_SAMPLE = 3  # Minimum deal count for a credible signal claim in the memo


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

    lines = [f"### M&A Market Snapshot, {year_range_str}", ""]

    # -------------------------------------------------------------------
    # SECTION 1: Market Regime
    # -------------------------------------------------------------------
    lines.append("**Market Regime**")
    current_regime = regime_mod.get_current_regime(filters)
    transition_text = regime_mod.regime_transition_summary(filters)
    regime_df_all = regime_mod.classify_regimes(filters)
    num_regime_years = len(regime_df_all) if not regime_df_all.empty else 0

    if num_regime_years >= 7:
        regime_confidence = "high confidence"
    elif num_regime_years >= 5:
        regime_confidence = "moderate confidence"
    elif num_regime_years >= 3:
        regime_confidence = "low confidence"
    else:
        regime_confidence = "insufficient data"

    if current_regime:
        label = current_regime.get("regime_label", "Indeterminate")
        year = current_regime.get("year")
        explanation = current_regime.get("explanation", "")

        regime_line = (
            f"Current regime ({year}): **{label}** "
            f"({regime_confidence}, based on {num_regime_years} years of data)."
        )
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
        # Only include sectors with sufficient data (n >= MIN_SAMPLE)
        credible_df = imbalance_df[imbalance_df["recent_deal_count"] >= MIN_SAMPLE]

        # Lead with the most actionable signals: Overheating and Cooling
        for signal in ["Overheating", "Cooling"]:
            signal_rows = credible_df[credible_df["signal"] == signal]
            for _, row in signal_rows.head(1).iterrows():
                sector = row["sector_name"]
                act = row["activity_momentum_pct"]
                val_mom = row["valuation_momentum_pct"]
                ev_now = row.get("recent_ev_median")
                n = int(row.get("recent_deal_count", 0))
                from ma.analytics.imbalance import signal_confidence as _sig_conf
                conf = _sig_conf(n)

                # Enrich with relative valuation percentile if available
                pct_str = ""
                if not rel_df.empty:
                    rel_row = rel_df[rel_df["sector_name"] == sector]
                    if not rel_row.empty:
                        pct = rel_row.iloc[0].get("historical_percentile")
                        premium = rel_row.iloc[0].get("premium_discount")
                        if pd.notna(pct):
                            pct_str = f", at the {pct:.0f}th percentile of its historical range"
                        if pd.notna(premium) and abs(premium) >= 0.5:
                            sign = "+" if premium > 0 else ""
                            pct_str += f" ({sign}{premium:.1f}x vs market)"

                ev_str = f" at {ev_now:.1f}x EV/EBITDA{pct_str}" if ev_now and not pd.isna(ev_now) else pct_str

                if signal == "Overheating":
                    lines.append(
                        f"**{sector}** shows late-cycle characteristics, activity up {act:.0f}% "
                        f"with multiples expanding {val_mom:+.0f}%{ev_str} "
                        f"(n={n}, {conf}). "
                        f"Investors entering auction processes here should stress-test under "
                        f"multiple compression scenarios."
                    )
                else:  # Cooling
                    lines.append(
                        f"**{sector}** is cooling, activity {act:.0f}% and multiples "
                        f"contracting{ev_str} (n={n}, {conf}). "
                        f"Potential relative value window; watch for motivated secondary sellers."
                    )

        # Add one Healthy Growth sector as a constructive signal
        healthy_rows = credible_df[credible_df["signal"] == "Healthy Growth"]
        for _, row in healthy_rows.head(1).iterrows():
            sector = row["sector_name"]
            act = row["activity_momentum_pct"]
            n = int(row.get("recent_deal_count", 0))
            from ma.analytics.imbalance import signal_confidence as _sig_conf
            conf = _sig_conf(n)
            lines.append(
                f"**{sector}** presents a constructive entry profile, rising activity "
                f"({act:+.0f}%) with contained multiples (n={n}, {conf}), "
                f"which suggests a healthy demand/price dynamic."
            )

        if credible_df.empty:
            lines.append("No sectors have sufficient deal history for high-confidence signal classification in the current period.")
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

    all_profiles = sponsor_profile_mod.generate_all_profiles(filters, min_deals=MIN_SAMPLE)
    sponsor_df = sponsor_intel.sponsor_rankings(filters, top_n=3)
    top_sponsors = sponsor_df["sponsor_name"].tolist() if not sponsor_df.empty else []

    if not all_profiles.empty:
        # Profile top 2-3 sponsors with deal count and valuation context
        profiled = []
        for _, prow in all_profiles.head(3).iterrows():
            sp_name = prow["sponsor_name"]
            stance = prow.get("valuation_stance", "Market Buyer")
            dc = int(prow.get("deal_count", 0))
            ev_cnt = int(prow.get("ev_ebitda_count", 0))
            sectors = prow.get("preferred_sectors", "")
            ev = prow.get("avg_ev_ebitda")

            if stance == "Insufficient valuation data":
                stance_str = f"**{sp_name}** ({dc} deals, insufficient valuation data for classification)"
            else:
                ev_part = f", avg EV/EBITDA **{ev:.1f}x**" if ev else ""
                stance_str = (
                    f"**{sp_name}** (**{stance}**, based on {ev_cnt} deals with valuation data{ev_part})"
                )

            if sectors:
                stance_str += f", concentrated in {sectors}"
            profiled.append(stance_str)

        lines.append(
            "; ".join(profiled) + ". "
            + f"Sponsor activity accounts for **{sponsor_pct:.0f}%** of deal count."
        )
    elif top_sponsors:
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

    # Count deals with valid EV/EBITDA for sample size disclosure
    ev_sample_count = int(df_all["ev_to_ebitda"].dropna().count()) if not df_all.empty else 0

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
        val_line = f"Market-wide median EV/EBITDA: **{current_median:.1f}x** (based on {ev_sample_count} deals with valid valuation data)"
        if prior_median and change_pct is not None:
            val_line += f", having {dir_label} from {prior_median:.1f}x ({change_pct:+.1f}%)"
        val_line += "."

        # Relative valuation context for market overall
        if direction == "expansion":
            val_line += " Investors should watch for earnings visibility risk if multiples outpace fundamentals."
        elif direction == "compression":
            val_line += " Multiple compression may be creating selective entry opportunities for patient capital."
        lines.append(val_line)
    elif ev_sample_count == 0:
        lines.append("No EV/EBITDA data available in the selected period for valuation analysis.")

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

    # Regime-driven watch item, only include if regime classification is credible
    if current_regime and regime_confidence in ("high confidence", "moderate confidence"):
        r_label = current_regime.get("regime_label", "")
        if r_label == "Peak / Late-Cycle":
            watch_items.append(
                f"Monitor for multiple compression as credit conditions evolve, exits may face narrower spread vs entry "
                f"({regime_confidence}, {num_regime_years} years of data)."
            )
        elif r_label == "Trough / Distressed":
            watch_items.append("Track credit market reopening signals, recovery vintages deploy when deal flow is still thin.")
        elif r_label == "Recovery / Opportunity":
            watch_items.append("Window may be closing, watch for deal count acceleration as a leading indicator that competition is intensifying.")

    # Overheating / Cooling sector watch, only if n >= MIN_SAMPLE (moderate or high confidence)
    if not imbalance_df.empty:
        from ma.analytics.imbalance import signal_confidence as _sig_conf
        credible_signals = imbalance_df[imbalance_df["recent_deal_count"] >= MIN_SAMPLE]

        for signal_type, watch_template in [
            ("Overheating", "overheating signals warrant monitoring for auction discipline and leverage underwriting standards"),
            ("Cooling", "cooling activity may surface secondary deal flow at improving prices"),
        ]:
            sig_rows = credible_signals[credible_signals["signal"] == signal_type]
            for _, sig_row in sig_rows.head(1).iterrows():
                sector = sig_row["sector_name"]
                n = int(sig_row.get("recent_deal_count", 0))
                conf = _sig_conf(n)
                if conf in ("high confidence", "moderate confidence"):
                    # Enrich with relative valuation if available
                    rel_note = ""
                    if not rel_df.empty:
                        rel_match = rel_df[rel_df["sector_name"] == sector]
                        if not rel_match.empty:
                            pct = rel_match.iloc[0].get("historical_percentile")
                            if pd.notna(pct):
                                rel_note = f", currently at the {pct:.0f}th percentile of its historical range"
                    watch_items.append(
                        f"**{sector}**{rel_note} with {signal_type} signal (n={n}, {conf}): {watch_template}."
                    )

    # Default watch items if none generated from data
    if not watch_items:
        if regime_confidence in ("low confidence", "insufficient data"):
            watch_items.append("No high-confidence watch items in the current period, expand the date range for more robust signal detection.")
        else:
            watch_items.append(f"**{most_active_sector}** deal flow concentration, monitor for sector rotation signals.")
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
