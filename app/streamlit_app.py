"""
M&A Database Dashboard. Six tabs, global sidebar filters.
Uses the finance-lab design system (style_inject + apply_plotly_theme).
"""
import os
import sys
from pathlib import Path

# Resolve project root and make it both import-visible AND the working directory,
# so relative paths in config.yaml (data/ma_database.duckdb, data/raw/real_deals.csv)
# resolve correctly on Streamlit Cloud regardless of how the app is launched.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ma.utils.config_loader import load_config
from ma.db.engine import init_db, get_connection
from ma.db.schema import create_schema
from ma.db import queries
from ma.analytics import valuation, market_activity, sponsor_intel, snapshot
from ma.analytics import regime as regime_mod
from ma.analytics import relative_valuation as rel_val_mod
from ma.analytics import imbalance as imbalance_mod
from ma.analytics import sponsor_profile as sponsor_profile_mod
from ma.analytics.execution import time_to_close_stats, deal_status_summary, completion_rate_by_deal_type
from ma.ingest.seed_real import seed_real_deals
from ma.ingest.seed_synthetic import seed_synthetic_deals
from ma.ingest.csv_import import preview_csv, import_csv
from ma.export.csv_export import export_deals_csv
from ma.export.excel_export import export_deals_excel
from ma.utils.formatting import fmt_currency, fmt_multiple, fmt_pct, quality_label, quality_color
from style_inject import (
    inject_styles, styled_header, styled_kpi, styled_card,
    styled_divider, styled_section_label, apply_plotly_theme, TOKENS,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="M&A Database",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_styles()

CONFIG = load_config("config.yaml")
COLORS = CONFIG["dashboard"]["chart_color_palette"]
H = CONFIG["dashboard"]["chart_height"]

# ---------------------------------------------------------------------------
# Semantic color system. Consistent across signals, regimes, quality tiers.
# ---------------------------------------------------------------------------
MIN_SAMPLE = 3  # Minimum deal count for a credible signal/classification

SIGNAL_COLORS = {
    "Overheating":       TOKENS["accent_danger"],
    "Narrowing":         TOKENS["accent_warning"],
    "Healthy Growth":    TOKENS["accent_primary"],
    "Cooling":           TOKENS["accent_info"],
    "Insufficient Data": TOKENS["text_secondary"],
}
REGIME_COLORS = {
    "Peak / Late-Cycle":      TOKENS["accent_danger"],
    "Recovery / Opportunity": TOKENS["accent_primary"],
    "Selective / Cautious":   TOKENS["accent_warning"],
    "Trough / Distressed":    TOKENS["accent_info"],
    "Indeterminate":          TOKENS["text_secondary"],
}
COMPLETENESS_COLORS = {
    "High":   TOKENS["accent_primary"],
    "Medium": TOKENS["accent_warning"],
    "Low":    TOKENS["accent_danger"],
}
CONFIDENCE_COLORS = {
    "high confidence":    TOKENS["accent_primary"],
    "moderate confidence":TOKENS["accent_warning"],
    "low confidence":     TOKENS["accent_warning"],
    "insufficient data":  TOKENS["text_secondary"],
}
STANCE_COLORS = {
    "Premium Buyer":               TOKENS["accent_danger"],
    "Value Buyer":                 TOKENS["accent_primary"],
    "Market Buyer":                TOKENS["text_secondary"],
    "Unknown":                     TOKENS["text_secondary"],
    "Insufficient valuation data": TOKENS["text_secondary"],
}


def confidence_badge(n: int) -> str:
    """Return a colored inline HTML span showing sample size and confidence level."""
    if n >= 10:
        level, color = "high confidence", CONFIDENCE_COLORS["high confidence"]
    elif n >= 5:
        level, color = "moderate confidence", CONFIDENCE_COLORS["moderate confidence"]
    elif n >= MIN_SAMPLE:
        level, color = "low confidence", CONFIDENCE_COLORS["low confidence"]
    else:
        level, color = "insufficient data", CONFIDENCE_COLORS["insufficient data"]
    return f'<span style="color:{color}; font-size:11px; font-weight:600;">n={n}, {level}</span>'


# ---------------------------------------------------------------------------
# DB init (cached) — creates schema + seeds from data/raw/real_deals.csv on
# first run. Cold-start safe: on Streamlit Cloud the .duckdb file does not
# exist in the repo (gitignored), so this block regenerates it end-to-end.
# ---------------------------------------------------------------------------
@st.cache_resource
def _init():
    db_path = PROJECT_ROOT / CONFIG["database"]["path"]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(str(db_path))
    create_schema()
    if queries.get_deals_count() == 0:
        real_inserted = seed_real_deals(CONFIG)
        cnt = queries.get_deals_count()
        syn_inserted = seed_synthetic_deals(CONFIG, cnt)
        return {"status": "seeded", "real": real_inserted, "synthetic": syn_inserted}
    return {"status": "existing", "count": queries.get_deals_count()}

try:
    _init()
except Exception as exc:  # surface init errors on Streamlit Cloud instead of
    # letting the next query blow up with a cryptic DuckDB error
    st.error(
        "Database initialization failed. This usually means the DuckDB file "
        "could not be created or the seed data could not be loaded.\n\n"
        f"**Error:** `{type(exc).__name__}: {exc}`"
    )
    st.stop()


# ---------------------------------------------------------------------------
# Cached data fetchers — avoids repeated SELECT * on every render
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def _get_all_deals(filter_key: str) -> "pd.DataFrame":
    """Cache the full deal table per filter state (5 min TTL)."""
    import json
    f = json.loads(filter_key)
    return queries.get_all_deals(f)

@st.cache_data(ttl=300)
def _get_kpi_summary(filter_key: str) -> dict:
    import json
    return queries.get_kpi_summary(json.loads(filter_key))

def _filter_key(f: dict) -> str:
    """Stable JSON key for a filter dict (for cache_data)."""
    import json
    return json.dumps(f, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
def build_filters() -> dict:
    """Build the global filter dict from sidebar widgets."""
    # Platform Methodology — collapsible, above filters
    with st.sidebar.expander("Platform Methodology", expanded=False):
        st.markdown("""
**Data Architecture**
- 5 relational tables (deals, parties, sectors, valuation_metrics, deal_metadata)
- 2 analytical views (v_deals_flat, v_deals_summary)
- DuckDB columnar engine: optimized for analytical aggregation

**Data Sources**
- Curated real transactions from public filings, press releases, and deal announcements
- Synthetic extension layer for analytical coverage (clearly labeled, separately filterable)

**Quality Scoring**
- Weighted completeness: Tier 1 (deal identity, wt 3.0) · Tier 2 (financials, wt 2.0) · Tier 3 (process, wt 1.0)
- Rule-based confidence: source verification × data origin × completeness threshold

**Analytics Engine**
- Regime detection (Peak / Recovery / Selective / Trough classification)
- Relative valuation (sector premium/discount vs market, historical percentile)
- Sponsor behavioral profiling (valuation stance, sector concentration, timing)
- Market imbalance detection (activity × valuation momentum signals)
""")

    with st.sidebar:
        styled_section_label("FILTERS")

    # Date range
    year_start, year_end = st.sidebar.slider(
        "Announcement Year", 2016, 2026, (2016, 2026)
    )

    # Sector
    all_sectors = [s["sector_name"] for s in CONFIG["sectors"]]
    sectors = st.sidebar.multiselect("Sector", all_sectors, default=[])

    # Sub-industry (dynamic based on selected sectors)
    _sector_source = sectors if sectors else all_sectors
    all_sub_industries = []
    for s in CONFIG["sectors"]:
        if s["sector_name"] in _sector_source:
            all_sub_industries.extend(s.get("sub_industries", []))
    sub_industries = st.sidebar.multiselect("Sub-industry", sorted(set(all_sub_industries)), default=[])

    # Deal type
    all_deal_types = ["lbo", "strategic_acquisition", "merger", "take_private", "carve_out"]
    deal_types = st.sidebar.multiselect("Deal Type", all_deal_types, default=[])

    # Acquirer type
    acq_types = st.sidebar.multiselect("Acquirer Type", ["sponsor", "strategic", "consortium", "other"], default=[])

    # Acquirer / sponsor name
    _all_acq_names = sorted(set(
        get_connection().execute("SELECT DISTINCT party_name FROM parties ORDER BY party_name").df()["party_name"].tolist()
    ))
    acq_names = st.sidebar.multiselect("Acquirer / Sponsor Name", _all_acq_names, default=[])

    # Deal status
    statuses = st.sidebar.multiselect("Deal Status", ["closed", "announced", "pending", "terminated"], default=[])

    # Geography
    _all_geos = sorted(set(
        get_connection().execute("SELECT DISTINCT geography FROM deals WHERE geography IS NOT NULL ORDER BY geography").df()["geography"].tolist()
    ))
    geographies = st.sidebar.multiselect("Geography", _all_geos, default=[])

    # Data origin — default to real so first impression shows verified data only
    data_origin = st.sidebar.radio("Data Origin", ["real", "all", "synthetic"], index=0)

    # Deal value range
    dv_min, dv_max = st.sidebar.slider("Deal Value ($M)", 0, 100000, (0, 100000), step=100)

    # Completeness threshold
    comp_min = st.sidebar.slider("Min Completeness %", 0, 100, 0)

    filters = {"year_start": year_start, "year_end": year_end}
    if sectors:
        filters["sectors"] = sectors
    if sub_industries:
        filters["sub_industries"] = sub_industries
    if deal_types:
        filters["deal_types"] = deal_types
    if acq_types:
        filters["acquirer_types"] = acq_types
    if acq_names:
        filters["acquirer_names"] = acq_names
    if statuses:
        filters["deal_statuses"] = statuses
    if geographies:
        filters["geographies"] = geographies
    if data_origin != "all":
        filters["data_origin"] = data_origin
    if dv_min > 0:
        filters["deal_value_min"] = float(dv_min)
    if dv_max < 100000:
        filters["deal_value_max"] = float(dv_max)
    if comp_min > 0:
        filters["completeness_min"] = float(comp_min)

    return filters


filters = build_filters()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
styled_header(
    "M&A Database and Analysis Tool",
    "Valuations | Sponsor Behavior | Market Activity Trends",
)
styled_divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tabs = st.tabs(["OVERVIEW", "VALUATION", "MARKET", "SPONSORS", "DEALS", "DATA"])

# ===========================================================================
# TAB 1: OVERVIEW
# ===========================================================================
with tabs[0]:
    kpis = _get_kpi_summary(_filter_key(filters))

    if not kpis:
        st.warning("No deals match the current filters.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1: styled_kpi("TOTAL DEALS", f"{kpis.get('total_deals', 0):,}")
        with c2: styled_kpi("TOTAL VALUE", fmt_currency(kpis.get("total_deal_value_usd"), decimals=1, suffix="B"))
        total = kpis.get("total_deals", 1) or 1
        sp_pct = kpis.get("sponsor_deal_count", 0) / total * 100
        st_pct = kpis.get("strategic_deal_count", 0) / total * 100
        with c3: styled_kpi("SPONSOR / STRATEGIC", f"{sp_pct:.0f}% / {st_pct:.0f}%")
        with c4: styled_kpi("MEDIAN EV/EBITDA", fmt_multiple(kpis.get("median_ev_to_ebitda")))

        c5, c6, c7, c8 = st.columns(4)
        with c5: styled_kpi("SPONSOR DEALS", f"{kpis.get('sponsor_deal_count', 0):,}")
        with c6: styled_kpi("STRATEGIC DEALS", f"{kpis.get('strategic_deal_count', 0):,}")
        with c7: styled_kpi("TOP SECTOR", kpis.get("most_active_sector", "n/a"))
        with c8: styled_kpi("TOP SPONSOR", kpis.get("most_active_sponsor", "n/a"))

        # Data Composition block. Makes real/synthetic separation visible.
        styled_divider()
        styled_section_label("DATA COMPOSITION")
        _real_n = kpis.get("real_deals", 0)
        _synth_n = kpis.get("synthetic_deals", 0)
        _total_n = kpis.get("total_deals", 1) or 1
        _real_pct = _real_n / _total_n * 100
        _synth_pct = _synth_n / _total_n * 100

        dc1, dc2, dc3 = st.columns(3)
        with dc1: styled_kpi("REAL DEALS", f"{_real_n:,}", delta=f"{_real_pct:.0f}% of universe", delta_color=TOKENS["text_secondary"])
        with dc2: styled_kpi("SYNTHETIC DEALS", f"{_synth_n:,}", delta=f"{_synth_pct:.0f}% of universe", delta_color=TOKENS["text_secondary"])
        with dc3: styled_kpi("COMBINED", f"{_total_n:,}", delta="All records", delta_color=TOKENS["text_secondary"])
        st.caption(
            "Data origin controlled via the sidebar filter. "
            "Analytics default to real transactions only. Use 'all' to include the synthetic extension layer."
        )

        styled_divider()

        col1, col2 = st.columns(2)

        with col1:
            cnt_df = queries.get_deal_count_by_year(filters)
            if not cnt_df.empty:
                fig = px.line(cnt_df, x="year", y="deal_count", title="Deal Count Over Time")
                fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count")
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            dt_df = queries.get_deal_type_distribution(filters)
            if not dt_df.empty:
                fig = px.pie(dt_df, names="deal_type", values="deal_count",
                             title="Deal Type Distribution", hole=0.65)
                fig.update_layout(height=H)
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            sec_df = queries.get_deal_count_by_sector(filters, top_n=10)
            if not sec_df.empty:
                fig = px.bar(sec_df.sort_values("deal_count"), x="deal_count", y="sector_name",
                             orientation="h", title="Top 10 Sectors by Deal Count")
                fig.update_layout(height=H, xaxis_title="Deal Count", yaxis_title="")
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

        with col4:
            acq_df = queries.get_deal_count_by_acquirer(filters, top_n=10)
            if not acq_df.empty:
                fig = px.bar(acq_df.sort_values("deal_count"), x="deal_count", y="acquirer_name",
                             orientation="h", title="Top 10 Acquirers by Deal Count",
                             color="acquirer_type")
                fig.update_layout(height=H, xaxis_title="Deal Count", yaxis_title="")
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

        # Market Regime Section
        styled_section_label("MARKET REGIME")
        regime_df = regime_mod.classify_regimes(filters)
        current_regime = regime_mod.get_current_regime(filters)

        if not regime_df.empty:
            regime_df["color"] = regime_df["regime_label"].map(REGIME_COLORS).fillna(TOKENS["text_secondary"])

            col_regime1, col_regime2 = st.columns([2, 1])
            with col_regime1:
                fig_regime = px.bar(
                    regime_df, x="year", y="deal_count",
                    color="regime_label",
                    color_discrete_map=REGIME_COLORS,
                    title="Market Regime Timeline (Annual Deal Count by Regime)",
                    labels={"deal_count": "Deal Count", "year": "Year", "regime_label": "Regime"},
                )
                fig_regime.update_layout(height=380, legend_title="Regime")
                apply_plotly_theme(fig_regime)
                st.plotly_chart(fig_regime, use_container_width=True)

            with col_regime2:
                if current_regime:
                    label = current_regime.get("regime_label", "n/a")
                    year = current_regime.get("year", "n/a")
                    explanation = current_regime.get("explanation", "")
                    color = REGIME_COLORS.get(label, TOKENS["text_secondary"])
                    styled_card(
                        f'<div style="font-size:0.65rem; color:{TOKENS["text_muted"]}; text-transform:uppercase; letter-spacing:0.1em; font-weight:600;">'
                        f'CURRENT REGIME ({year})</div>'
                        f'<div style="font-size:1.25rem; font-weight:600; color:{color}; margin:0.4rem 0; font-family:{TOKENS["font_display"]};">{label}</div>'
                        f'<div style="font-size:0.8rem; color:{TOKENS["text_secondary"]}; line-height:1.5;">{explanation}</div>',
                        accent_color=color,
                    )
                    transition = regime_mod.regime_transition_summary(filters)
                    if transition:
                        st.caption(transition)

        styled_divider()

        # M&A Market Snapshot
        styled_section_label("M&A MARKET SNAPSHOT")
        with st.expander("Auto-generated analyst memo", expanded=True):
            memo = snapshot.generate_snapshot(filters, CONFIG)
            styled_card(memo, accent_color=TOKENS["accent_primary"])

        # Real vs synthetic composition callout
        audit_df = queries.get_data_origin_audit()
        if not audit_df.empty:
            audit_map = {r["data_origin"]: r["deal_count"] for r in audit_df.to_dict("records")}
            real_n = audit_map.get("real", 0)
            syn_n = audit_map.get("synthetic", 0)
            st.caption(f"Data composition: {real_n} verified real deals | {syn_n} synthetic extension. "
                       f"Use the Data Origin filter to view real only.")


# ===========================================================================
# TAB 2: VALUATION
# ===========================================================================
with tabs[1]:
    styled_section_label("VALUATION ANALYSIS")
    st.caption("Sector multiples, premium dynamics, and valuation regime shifts.")

    # EV/EBITDA box plot. Full-width primary chart.
    ev_df = valuation.ev_ebitda_by_sector(filters)
    if not ev_df.empty:
        _ev_counts = ev_df.groupby("sector_name")["ev_to_ebitda"].count()
        _ev_included = _ev_counts[_ev_counts >= MIN_SAMPLE].index.tolist()
        _ev_excluded = _ev_counts[_ev_counts < MIN_SAMPLE].index.tolist()
        _ev_caution = _ev_counts[(_ev_counts >= MIN_SAMPLE) & (_ev_counts <= 5)].index.tolist()
        ev_df_f = ev_df[ev_df["sector_name"].isin(_ev_included)]
        if not ev_df_f.empty:
            fig = px.box(ev_df_f, x="sector_name", y="ev_to_ebitda",
                         title="EV/EBITDA Distribution by Sector")
            fig.update_layout(height=H, xaxis_title="Sector", yaxis_title="EV/EBITDA (x)",
                              xaxis_tickangle=-30)
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
            captions = []
            if _ev_excluded:
                captions.append(f"Excluded (< {MIN_SAMPLE} obs): {', '.join(_ev_excluded)}.")
            if _ev_caution:
                captions.append(f"Interpret with caution (3 to 5 obs): {', '.join(_ev_caution)}.")
            if captions:
                st.caption(" ".join(captions))

    col1, col2 = st.columns(2)

    with col1:
        rev_df = valuation.ev_revenue_by_sector(filters)
        if not rev_df.empty:
            _rev_counts = rev_df.groupby("sector_name")["ev_to_revenue"].count()
            _rev_included = _rev_counts[_rev_counts >= MIN_SAMPLE].index.tolist()
            _rev_excluded = _rev_counts[_rev_counts < MIN_SAMPLE].index.tolist()
            rev_df_f = rev_df[rev_df["sector_name"].isin(_rev_included)]
            if not rev_df_f.empty:
                fig = px.box(rev_df_f, x="sector_name", y="ev_to_revenue",
                             title="EV/Revenue Distribution by Sector")
                fig.update_layout(height=H, xaxis_tickangle=-30, xaxis_title="Sector",
                                  yaxis_title="EV/Revenue (x)")
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
                if _rev_excluded:
                    st.caption(f"Excluded (< {MIN_SAMPLE} obs): {', '.join(_rev_excluded)}.")

    with col2:
        prem_df = valuation.premium_distribution(filters)
        if not prem_df.empty:
            fig = px.histogram(prem_df, x="premium_paid_pct",
                               title="Premium Paid Distribution (Public Targets)",
                               nbins=30)
            fig.update_layout(height=H, xaxis_title="Premium Paid (%)", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    regime_df = valuation.median_ev_ebitda_by_sector_year(filters)
    if not regime_df.empty:
        fig = px.line(regime_df, x="year", y="median_ev_to_ebitda", color="sector_name",
                      title="Median EV/EBITDA Over Time by Sector")
        fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Median EV/EBITDA (x)")
        apply_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    spvs = valuation.sponsor_vs_strategic_multiples(filters)
    if not spvs.empty:
        fig = px.bar(spvs, x="acquirer_type", y=["avg_ev_to_ebitda", "median_ev_to_ebitda"],
                     barmode="group",
                     title="Sponsor vs Strategic Entry EV/EBITDA")
        fig.update_layout(height=320, xaxis_title="Acquirer Type", yaxis_title="EV/EBITDA (x)")
        apply_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    stats = valuation.sector_valuation_stats(filters)
    if not stats.empty:
        with st.expander("EV/EBITDA Summary Statistics by Sector", expanded=False):
            st.dataframe(stats.style.format({
                "median": "{:.1f}x", "mean": "{:.1f}x", "p25": "{:.1f}x", "p75": "{:.1f}x"
            }), use_container_width=True)

    # Relative Valuation Section
    styled_divider()
    styled_section_label("RELATIVE VALUATION")
    st.caption("Sector premium / discount vs market median, historical percentile, and sponsor vs strategic spread.")

    rel_df = rel_val_mod.sector_relative_valuation(filters)

    if not rel_df.empty:
        # rel_df is already filtered to >= MIN_SAMPLE in relative_valuation.py
        col_rel1, col_rel2 = st.columns(2)

        with col_rel1:
            fig_rel = px.bar(
                rel_df.sort_values("premium_discount"),
                x="premium_discount", y="sector_name",
                orientation="h",
                color="premium_discount",
                color_continuous_scale=[TOKENS["accent_danger"], TOKENS["bg_elevated"], TOKENS["accent_primary"]],
                color_continuous_midpoint=0,
                title="Sector EV/EBITDA Premium / Discount vs Market",
                labels={"premium_discount": "Premium / Discount (x)", "sector_name": ""},
                text="premium_discount",
            )
            fig_rel.update_traces(texttemplate="%{text:+.1f}x", textposition="outside")
            fig_rel.update_layout(height=H, coloraxis_showscale=False,
                                  xaxis_title="vs Market Median (x)")
            apply_plotly_theme(fig_rel)
            st.plotly_chart(fig_rel, use_container_width=True)
            st.caption(f"Only sectors with at least {MIN_SAMPLE} valid EV/EBITDA observations shown.")

        with col_rel2:
            rel_pct_df = rel_df.dropna(subset=["historical_percentile"]).sort_values("historical_percentile", ascending=False)
            if not rel_pct_df.empty:
                fig_pct = px.bar(
                    rel_pct_df,
                    x="historical_percentile", y="sector_name",
                    orientation="h",
                    color="historical_percentile",
                    color_continuous_scale=[TOKENS["accent_primary"], TOKENS["accent_warning"], TOKENS["accent_danger"]],
                    color_continuous_midpoint=50,
                    title="Sector EV/EBITDA Historical Percentile",
                    labels={"historical_percentile": "Percentile", "sector_name": ""},
                    text="historical_percentile",
                )
                fig_pct.update_traces(texttemplate="%{text:.0f}th", textposition="outside")
                fig_pct.update_layout(height=H, coloraxis_showscale=False,
                                      xaxis_title="Historical Percentile (0 to 100th)")
                apply_plotly_theme(fig_pct)
                st.plotly_chart(fig_pct, use_container_width=True)
                st.caption(f"Only sectors with at least {MIN_SAMPLE} valid observations shown.")

        sv_df = rel_val_mod.sponsor_vs_strategic_premium(filters)
        if not sv_df.empty:
            fig_sv = px.bar(
                sv_df.sort_values("spread"),
                x="spread", y="sector_name",
                orientation="h",
                color="spread",
                color_continuous_scale=[TOKENS["accent_info"], TOKENS["bg_elevated"], TOKENS["accent_primary"]],
                color_continuous_midpoint=0,
                title="Sponsor vs Strategic Entry Premium by Sector (EV/EBITDA Spread)",
                labels={"spread": "Sponsor minus Strategic (x)", "sector_name": ""},
                text="spread",
            )
            fig_sv.update_traces(texttemplate="%{text:+.1f}x", textposition="outside")
            fig_sv.update_layout(height=H, coloraxis_showscale=False,
                                 xaxis_title="Spread (x)")
            apply_plotly_theme(fig_sv)
            st.plotly_chart(fig_sv, use_container_width=True)
            st.caption(f"Only sectors where both sponsor and strategic have at least {MIN_SAMPLE} deals with valid EV/EBITDA.")

        narrative = rel_val_mod.relative_valuation_narrative(filters)
        if narrative:
            styled_card(narrative, accent_color=TOKENS["accent_info"])

        with st.expander("Relative Valuation Detail Table"):
            display_rel = rel_df[["sector_name", "deal_count", "sector_median",
                                   "market_median", "premium_discount",
                                   "historical_percentile", "interpretation_label"]].copy()
            st.dataframe(display_rel.style.format({
                "sector_median": "{:.1f}x",
                "market_median": "{:.1f}x",
                "premium_discount": "{:+.1f}x",
                "historical_percentile": "{:.0f}th",
            }, na_rep="n/a"), use_container_width=True)


# ===========================================================================
# TAB 3: MARKET ACTIVITY
# ===========================================================================
with tabs[2]:
    styled_section_label("MARKET ACTIVITY")
    st.caption("Volume trends, sector heatmaps, sponsor vs strategic mix.")

    col1, col2 = st.columns(2)

    with col1:
        cnt_df = market_activity.deal_count_over_time(filters)
        if not cnt_df.empty:
            fig = px.area(cnt_df, x="year", y="deal_count", title="Annual Deal Count")
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        val_df = market_activity.deal_value_over_time(filters)
        if not val_df.empty:
            val_df["total_value_B"] = val_df["total_value_usd"] / 1000
            fig = px.area(val_df, x="year", y="total_value_B", title="Annual Deal Value")
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Value ($B)")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Full-width sector heatmap
    heatmap_df = market_activity.sector_activity_heatmap(filters)
    if not heatmap_df.empty and "sector_name" in heatmap_df.columns:
        year_cols = [c for c in heatmap_df.columns if str(c).isdigit() or (isinstance(c, (int, float)))]
        if year_cols:
            z = heatmap_df[year_cols].values
            fig = go.Figure(data=go.Heatmap(
                z=z,
                x=[str(int(c)) for c in year_cols],
                y=heatmap_df["sector_name"].tolist(),
                colorscale=[[0.0, TOKENS["bg_elevated"]], [1.0, TOKENS["accent_primary"]]],
                showscale=True,
            ))
            fig.update_layout(title="Sector Activity Heatmap (Deal Count by Year)",
                              height=H + 100, xaxis_title="Year", yaxis_title="Sector")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        svs_df = market_activity.sponsor_vs_strategic_trend(filters)
        if not svs_df.empty:
            fig = px.bar(svs_df, x="year", y="deal_count", color="acquirer_type",
                         barmode="stack", title="Sponsor vs Strategic Activity Over Time")
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        status_df = market_activity.deal_status_breakdown(filters)
        if not status_df.empty:
            fig = px.bar(status_df, x="year", y="deal_count", color="deal_status",
                         barmode="stack", title="Deal Status Breakdown Over Time")
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    tree_df = market_activity.sector_value_treemap(filters)
    if not tree_df.empty and "total_value_usd" in tree_df.columns:
        with st.expander("Deal Value by Sector (Treemap)", expanded=False):
            fig = px.treemap(tree_df, path=["sector_name"], values="total_value_usd",
                             title="Deal Value by Sector ($M)")
            fig.update_layout(height=480)
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Market Signals Section
    styled_divider()
    styled_section_label("MARKET SIGNALS | SECTOR IMBALANCE")
    st.caption(
        "Activity momentum vs valuation momentum by sector. "
        "Each sector is classified into one of four quadrants: Overheating, Healthy Growth, Narrowing, or Cooling."
    )

    imbalance_df = imbalance_mod.detect_sector_imbalances(filters)

    if not imbalance_df.empty:
        heat_df = imbalance_mod.market_heat_map(filters)
        if not heat_df.empty:
            fig_scatter = px.scatter(
                heat_df,
                x="activity_momentum_pct",
                y="valuation_momentum_pct",
                color="signal",
                color_discrete_map=SIGNAL_COLORS,
                text="sector_name",
                title="Sector Quadrant | Activity Momentum vs Valuation Momentum",
                labels={
                    "activity_momentum_pct": "Activity Momentum (% change in deal count)",
                    "valuation_momentum_pct": "Valuation Momentum (% change in EV/EBITDA)",
                },
            )
            fig_scatter.update_traces(textposition="top center", marker=dict(size=12))
            fig_scatter.add_hline(y=0, line_dash="dash", line_color=TOKENS["text_muted"], line_width=1)
            fig_scatter.add_vline(x=0, line_dash="dash", line_color=TOKENS["text_muted"], line_width=1)
            fig_scatter.update_layout(height=480)
            apply_plotly_theme(fig_scatter)
            st.plotly_chart(fig_scatter, use_container_width=True)

        # Signal table — add n (recent deal count) and confidence qualifier
        from ma.analytics.imbalance import signal_confidence as _sig_conf
        display_imbalance = imbalance_df[[
            "sector_name", "recent_deal_count", "activity_momentum_pct",
            "valuation_momentum_pct", "recent_ev_median", "signal",
        ]].copy()
        display_imbalance["confidence"] = display_imbalance["recent_deal_count"].apply(_sig_conf)
        display_imbalance.columns = [
            "Sector", "n (recent deals)", "Activity Momentum (%)", "Valuation Momentum (%)",
            "Recent Median EV/EBITDA", "Signal", "Confidence",
        ]

        def _color_signal(val):
            return f"color: {SIGNAL_COLORS.get(val, TOKENS['text_secondary'])}"

        def _color_confidence(val):
            return f"color: {CONFIDENCE_COLORS.get(val, TOKENS['text_secondary'])}"

        styled = (
            display_imbalance.style
            .map(_color_signal, subset=["Signal"])
            .map(_color_confidence, subset=["Confidence"])
            .format({
                "Activity Momentum (%)": "{:+.1f}%",
                "Valuation Momentum (%)": "{:+.1f}%",
                "Recent Median EV/EBITDA": "{:.1f}x",
            }, na_rep="n/a")
        )
        st.dataframe(styled, use_container_width=True)
        st.caption(f"'n (recent deals)' is the deal count in the most recent 2-year window. Signals with n < {MIN_SAMPLE} are classified as Insufficient Data.")

        narrative = imbalance_mod.imbalance_narrative(filters)
        if narrative:
            styled_section_label("SECTOR SIGNAL NARRATIVE")
            styled_card(narrative, accent_color=SIGNAL_COLORS["Narrowing"])
    else:
        st.info("Sector imbalance analysis requires at least 5 years of data. Expand the date range to enable this analysis.")


# ===========================================================================
# TAB 4: SPONSOR INTELLIGENCE
# ===========================================================================
with tabs[3]:
    styled_section_label("SPONSOR INTELLIGENCE")
    st.caption("PE sponsor rankings, sector preferences, and entry multiples.")

    rankings = sponsor_intel.sponsor_rankings(filters, top_n=15)

    col1, col2 = st.columns(2)

    with col1:
        if not rankings.empty:
            fig = px.bar(rankings.sort_values("deal_count"), x="deal_count", y="sponsor_name",
                         orientation="h", title="Top Sponsors by Deal Count")
            fig.update_layout(height=H, xaxis_title="Deal Count", yaxis_title="")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_value = sponsor_intel.top_sponsors_by_value(filters, top_n=15)
        if not top_value.empty:
            top_value["total_B"] = top_value["total_deal_value_usd"] / 1000
            fig = px.bar(top_value.sort_values("total_B"), x="total_B", y="sponsor_name",
                         orientation="h", title="Top Sponsors by Total Deal Value")
            fig.update_layout(height=H, xaxis_title="Total Deal Value ($B)", yaxis_title="")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Full-width sponsor sector heatmap
    sec_heat = sponsor_intel.sponsor_sector_heatmap(filters, top_n_sponsors=12)
    if not sec_heat.empty and "sponsor_name" in sec_heat.columns:
        sector_cols = [c for c in sec_heat.columns if c != "sponsor_name"]
        if sector_cols:
            fig = go.Figure(data=go.Heatmap(
                z=sec_heat[sector_cols].values,
                x=sector_cols,
                y=sec_heat["sponsor_name"].tolist(),
                colorscale=[[0.0, TOKENS["bg_elevated"]], [1.0, TOKENS["accent_primary"]]],
            ))
            fig.update_layout(title="Sponsor Sector Preferences (Deal Count Heatmap)",
                              height=H + 100, xaxis_tickangle=-30,
                              xaxis_title="Sector", yaxis_title="Sponsor")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        entry_df = sponsor_intel.sponsor_entry_multiples(filters, top_n=15)
        if not entry_df.empty:
            _ev_count_col = "ev_count" if "ev_count" in entry_df.columns else None
            if _ev_count_col:
                _entry_incl = entry_df[entry_df[_ev_count_col] >= MIN_SAMPLE]
                _entry_excl = entry_df[entry_df[_ev_count_col] < MIN_SAMPLE]["sponsor_name"].tolist()
            else:
                _entry_incl = entry_df
                _entry_excl = []
            if not _entry_incl.empty:
                fig = px.bar(_entry_incl.sort_values("avg_ev_to_ebitda"), x="avg_ev_to_ebitda", y="sponsor_name",
                             orientation="h", title="Average Entry EV/EBITDA by Sponsor")
                fig.update_layout(height=H, xaxis_title="Avg EV/EBITDA (x)", yaxis_title="")
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
                if _entry_excl:
                    st.caption(f"Excluded (< {MIN_SAMPLE} deals with valid EV/EBITDA): {', '.join(_entry_excl)}.")

    with col4:
        trend_df = sponsor_intel.sponsor_deal_trend(filters, top_n_sponsors=5)
        if not trend_df.empty:
            fig = px.line(trend_df, x="year", y="deal_count", color="sponsor_name",
                          title="Top Sponsors | Annual Deal Count")
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    if not rankings.empty:
        with st.expander("Sponsor Rankings Table", expanded=False):
            rankings_display = rankings.copy()
            if "total_deal_value_usd" in rankings_display.columns:
                rankings_display["Total Value ($B)"] = rankings_display["total_deal_value_usd"] / 1000
                rankings_display.drop(columns=["total_deal_value_usd"], inplace=True)
            if "avg_deal_size_usd" in rankings_display.columns:
                rankings_display["Avg Deal Size ($B)"] = rankings_display["avg_deal_size_usd"] / 1000
                rankings_display.drop(columns=["avg_deal_size_usd"], inplace=True)
            st.dataframe(rankings_display.style.format({
                "Total Value ($B)": "${:.1f}B",
                "Avg Deal Size ($B)": "${:.1f}B",
                "avg_ev_to_ebitda": "{:.1f}x",
            }), use_container_width=True)

    # Sponsor Profiles Section
    styled_divider()
    styled_section_label("SPONSOR BEHAVIORAL PROFILES")
    st.caption("Valuation stance, sector concentration, and deal size profile for each sponsor with 3 or more deals.")

    all_profiles_df = sponsor_profile_mod.generate_all_profiles(filters, min_deals=3)

    if not all_profiles_df.empty:
        def _color_stance(val):
            return f"color: {STANCE_COLORS.get(val, TOKENS['text_secondary'])}"

        display_profiles = all_profiles_df.copy()
        fmt_cols = {"avg_ev_ebitda": "{:.1f}x", "ev_premium_vs_market": "{:+.1f}x"}
        style_cols = ["valuation_stance"]
        st.dataframe(
            display_profiles.style
            .map(_color_stance, subset=style_cols)
            .format(fmt_cols, na_rep="n/a"),
            use_container_width=True,
        )
        st.caption(f"Valuation stance requires at least {MIN_SAMPLE} deals with valid EV/EBITDA. 'Insufficient valuation data' shown otherwise.")

        # Drill-down profile card
        styled_section_label("SPONSOR PROFILE CARD")
        eligible_sponsors = all_profiles_df["sponsor_name"].tolist()
        selected_sponsor = st.selectbox("Select a sponsor for detailed profile", eligible_sponsors)

        if selected_sponsor:
            profile = sponsor_profile_mod.generate_sponsor_profile(selected_sponsor, filters)
            if profile:
                stance = profile.get("valuation_stance", "Unknown")
                stance_color = STANCE_COLORS.get(stance, TOKENS["text_secondary"])
                narrative = profile.get("narrative", "")
                sectors = ", ".join(profile.get("preferred_sectors", [])[:3]) or "n/a"
                ev = profile.get("avg_ev_ebitda")
                premium = profile.get("ev_premium_vs_market")
                deal_size = profile.get("avg_deal_size_usd")
                size_stance = profile.get("deal_size_stance", "n/a")
                ev_cnt = profile.get("ev_ebitda_count", 0)
                deal_cnt = profile.get("deal_count", 0)

                cp1, cp2, cp3, cp4 = st.columns(4)
                with cp1: styled_kpi("STANCE", stance)
                with cp2: styled_kpi("AVG ENTRY EV/EBITDA", f"{ev:.1f}x" if ev else "n/a")
                with cp3: styled_kpi("VS MARKET", f"{premium:+.1f}x" if premium is not None else "n/a")
                with cp4: styled_kpi("AVG DEAL SIZE", f"${deal_size/1000:.1f}B" if deal_size else "n/a")

                ev_badge = confidence_badge(ev_cnt)
                stance_note = (
                    f"Valuation classification based on {ev_cnt} of {deal_cnt} deals with valid EV/EBITDA. "
                    + ev_badge
                )
                st.markdown(stance_note, unsafe_allow_html=True)
                st.markdown(f"**Preferred Sectors:** {sectors} &nbsp;&nbsp; **Deal Size Profile:** {size_stance}",
                            unsafe_allow_html=True)
                styled_card(narrative, accent_color=stance_color)
    else:
        st.info("No sponsors with 3 or more deals found under current filters.")


# ===========================================================================
# TAB 5: DEAL EXPLORER
# ===========================================================================
with tabs[4]:
    styled_section_label("DEAL EXPLORER")
    st.caption("Full filterable deal table. Synthetic records are labeled.")

    deals_df = _get_all_deals(_filter_key(filters))

    if deals_df.empty:
        st.info("No deals match the current filters.")
    else:
        st.markdown(f"**{len(deals_df):,} deals** matching current filters.")

        # Display columns with human-readable labels
        _col_map = {
            "announcement_date": "Announced",
            "target_name": "Target",
            "acquirer_name": "Acquirer",
            "acquirer_type": "Acq. Type",
            "deal_type": "Deal Type",
            "deal_status": "Status",
            "deal_value_usd": "Deal Value ($M)",
            "ev_to_ebitda": "EV/EBITDA",
            "sector_name": "Sector",
            "geography": "Geography",
            "data_origin": "Origin",
            "completeness_score": "Completeness",
        }
        display_cols = [c for c in _col_map if c in deals_df.columns]
        display_df = deals_df[display_cols].rename(columns=_col_map).copy()

        # Highlight synthetic rows
        def highlight_synthetic(row):
            if row.get("Origin") == "synthetic":
                return [f"background-color: {TOKENS['bg_elevated']}"] * len(row)
            return [""] * len(row)

        st.dataframe(
            display_df.style.apply(highlight_synthetic, axis=1).format({
                "Deal Value ($M)": "${:,.0f}M",
                "EV/EBITDA": "{:.1f}x",
                "Completeness": "{:.0f}%",
            }, na_rep="n/a"),
            use_container_width=True,
            height=500,
        )

        styled_section_label("DEAL DETAIL")
        deal_ids = deals_df["deal_id"].tolist()
        target_names = deals_df["target_name"].tolist()
        options = [f"{t} ({d[:8]}...)" for t, d in zip(target_names, deal_ids)]
        selected_idx = st.selectbox("Select a deal to expand", range(len(options)),
                                    format_func=lambda i: options[i])

        if selected_idx is not None:
            row = deals_df.iloc[selected_idx]
            is_synthetic = row.get("data_origin") == "synthetic"
            if is_synthetic:
                st.warning("SYNTHETIC RECORD. This deal is simulated and does not represent a real transaction.")
            with st.expander(f"Deal Detail: {row.get('target_name')}", expanded=True):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown(f"**Target:** {row.get('target_name')}")
                    st.markdown(f"**Acquirer:** {row.get('acquirer_name', 'n/a')}")
                    st.markdown(f"**Type:** {row.get('deal_type')}")
                    st.markdown(f"**Status:** {row.get('deal_status')}")
                with c2:
                    st.markdown(f"**Deal Value:** {fmt_currency(row.get('deal_value_usd'))}")
                    st.markdown(f"**EV/EBITDA:** {fmt_multiple(row.get('ev_to_ebitda'))}")
                    st.markdown(f"**EV/Revenue:** {fmt_multiple(row.get('ev_to_revenue'))}")
                    st.markdown(f"**Premium Paid:** {fmt_pct(row.get('premium_paid_pct'))}")
                with c3:
                    st.markdown(f"**Sector:** {row.get('sector_name', 'n/a')}")
                    st.markdown(f"**Announced:** {row.get('announcement_date')}")
                    st.markdown(f"**Closed:** {row.get('closing_date', 'n/a')}")
                    st.markdown(f"**Completeness:** {fmt_pct(row.get('completeness_score'))}")

        styled_section_label("EXPORT")
        col_a, col_b, _ = st.columns([1, 1, 4])
        with col_a:
            if st.button("Export CSV"):
                path = export_deals_csv(filters, CONFIG)
                st.success(f"Exported: {path}")
        with col_b:
            if st.button("Export Excel"):
                path = export_deals_excel(filters, CONFIG)
                st.success(f"Exported: {path}")


# ===========================================================================
# TAB 6: DATA MANAGEMENT
# ===========================================================================
with tabs[5]:
    styled_section_label("DATA MANAGEMENT")

    # System Architecture
    styled_section_label("SYSTEM ARCHITECTURE")
    st.caption("Pipeline flow: Ingest | Validate | Score | Store | Analyze | Export.")

    arch_blocks = [
        ("INGESTION",  "Real deal seed loader. Synthetic generator. CSV bulk import. Form-based entry."),
        ("VALIDATION", "Required field checks. Type and range validation. Duplicate detection. Future-date guard."),
        ("SCORING",    "Weighted completeness. Rule-based confidence. Quality tiers. Low-completeness cap."),
        ("STORAGE",    "DuckDB columnar engine. 5 normalized tables. 2 analytical views. Single-file deployment."),
        ("ANALYTICS",  "Valuation and activity trends. Regime and imbalance detection. Sponsor profiling. Relative valuation."),
        ("EXPORT",     "Filtered CSV download. Formatted Excel multi-sheet. PDF report (phase 2). API connectors (future)."),
    ]
    arch_row1 = st.columns(3)
    for col, (lbl, txt) in zip(arch_row1, arch_blocks[:3]):
        with col:
            styled_card(
                f'<div style="font-size:0.65rem; color:{TOKENS["accent_primary"]}; text-transform:uppercase; letter-spacing:0.1em; font-weight:600; margin-bottom:0.4rem;">{lbl}</div>'
                f'<div style="font-size:0.8rem; color:{TOKENS["text_secondary"]}; line-height:1.5;">{txt}</div>'
            )
    arch_row2 = st.columns(3)
    for col, (lbl, txt) in zip(arch_row2, arch_blocks[3:]):
        with col:
            styled_card(
                f'<div style="font-size:0.65rem; color:{TOKENS["accent_primary"]}; text-transform:uppercase; letter-spacing:0.1em; font-weight:600; margin-bottom:0.4rem;">{lbl}</div>'
                f'<div style="font-size:0.8rem; color:{TOKENS["text_secondary"]}; line-height:1.5;">{txt}</div>'
            )

    styled_divider()

    mgmt_tabs = st.tabs(["ADD DEAL", "BULK IMPORT", "COMPLETENESS", "ORIGIN"])

    # Add Deal Form
    with mgmt_tabs[0]:
        styled_section_label("ADD NEW DEAL")
        with st.form("add_deal_form"):
            tc1, tc2, tc3 = st.columns(3)
            with tc1:
                target_name = st.text_input("Target Name *")
                acquirer_name_input = st.text_input("Acquirer Name")
                acquirer_type_input = st.selectbox("Acquirer Type", ["strategic", "sponsor", "consortium", "other"])
            with tc2:
                deal_type_input = st.selectbox("Deal Type *", ["strategic_acquisition", "lbo", "merger", "take_private", "carve_out"])
                deal_status_input = st.selectbox("Deal Status *", ["closed", "announced", "pending", "terminated"])
                announcement_date_input = st.date_input("Announcement Date *")
            with tc3:
                deal_value_input = st.number_input("Deal Value ($M)", min_value=0.0, value=0.0)
                sector_input = st.selectbox("Sector", [s["sector_name"] for s in CONFIG["sectors"]])
                data_origin_input = st.selectbox("Data Origin *", ["real", "synthetic"])

            notes_input = st.text_area("Notes")
            submitted = st.form_submit_button("Add Deal")

            if submitted:
                if not target_name or not announcement_date_input:
                    st.error("Target Name and Announcement Date are required.")
                else:
                    from ma.ingest.csv_import import import_csv
                    import tempfile, os, csv
                    row = {
                        "target_name": target_name,
                        "acquirer_name": acquirer_name_input,
                        "acquirer_type": acquirer_type_input,
                        "deal_type": deal_type_input,
                        "deal_status": deal_status_input,
                        "announcement_date": str(announcement_date_input),
                        "deal_value_usd": deal_value_input if deal_value_input > 0 else None,
                        "sector_name": sector_input,
                        "data_origin": data_origin_input,
                        "notes": notes_input,
                    }
                    # Write temp CSV and import
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                        writer.writeheader()
                        writer.writerow(row)
                        tmpfile = f.name
                    result = import_csv(tmpfile, CONFIG)
                    os.unlink(tmpfile)
                    if result["inserted"] > 0:
                        st.success("Deal added successfully!")
                        st.cache_resource.clear()
                    else:
                        st.error(f"Failed: {result['errors']}")

    # Bulk CSV Import
    with mgmt_tabs[1]:
        styled_section_label("BULK CSV IMPORT")
        st.caption("Upload a CSV with deal records. Required columns: target_name, announcement_date, deal_type, deal_status, data_origin.")
        uploaded = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded:
            import tempfile
            with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
                f.write(uploaded.read())
                tmpfile = f.name

            preview = preview_csv(tmpfile, CONFIG)
            st.markdown(f"**{preview['total_rows']} rows** | **{preview['valid_rows']} valid** | **{len(preview['invalid_rows'])} invalid** | **{len(preview['duplicate_warnings'])} duplicate warnings**.")

            if preview["invalid_rows"]:
                st.warning("Invalid rows (will be skipped):")
                st.json(preview["invalid_rows"])

            if preview["duplicate_warnings"]:
                st.warning(f"{len(preview['duplicate_warnings'])} possible duplicate(s) detected.")

            st.markdown("**Preview (first 20 rows):**")
            st.dataframe(preview["preview_df"], use_container_width=True)

            if st.button("Import (skip invalid rows)"):
                result = import_csv(tmpfile, CONFIG, skip_invalid=True)
                st.success(f"Inserted: {result['inserted']} | Skipped: {result['skipped']}")
                import os; os.unlink(tmpfile)

    # Completeness Audit
    with mgmt_tabs[2]:
        styled_section_label("COMPLETENESS AUDIT")

        with st.expander("Scoring Methodology", expanded=False):
            st.markdown("""
**Completeness Scoring Methodology**

Each deal is scored on a weighted scale based on field importance for M&A analysis:

- **Tier 1 (weight 3.0)**: Deal identity. Announcement date, acquirer, target, deal type, sector, deal value, status.
- **Tier 2 (weight 2.0)**: Analytical value. EV/EBITDA, EV/Revenue, premium paid, enterprise value, target EBITDA, target revenue, closing date, geography.
- **Tier 3 (weight 1.0)**: Secondary. Financing structure, leverage, hostile/friendly, notes, source URL, sub-industry.

**Quality tiers:** High (>= 80%) | Medium (50 to 79%) | Low (< 50%).

**Confidence** is scored separately: based on data origin (real vs synthetic) and source verification.
Low-completeness records (below 30%) are capped at 0.3 confidence regardless of source quality.

Low-quality records can be filtered out using the **Completeness Threshold** slider in the sidebar.
""")

        comp_df = queries.get_completeness_distribution(filters)
        if comp_df.empty:
            st.info("No data.")
        else:
            fig = px.histogram(comp_df, x="completeness_score", nbins=20,
                               title="Completeness Score Distribution")
            fig.update_layout(height=320, xaxis_title="Completeness Score (%)", yaxis_title="Deal Count")
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

            low_q = comp_df[comp_df["completeness_score"] < 50]
            st.markdown(f"**{len(low_q)} low-quality deals** (completeness < 50%):")
            if not low_q.empty:
                st.dataframe(low_q[["target_name", "data_origin", "completeness_score", "confidence_score"]]
                             .sort_values("completeness_score"), use_container_width=True)

    # Data Origin Audit
    with mgmt_tabs[3]:
        styled_section_label("DATA ORIGIN AUDIT")
        audit = queries.get_data_origin_audit()
        if not audit.empty:
            fig = px.pie(audit, names="data_origin", values="deal_count",
                         title="Real vs Synthetic Records", hole=0.65)
            fig.update_layout(height=320)
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(audit, use_container_width=True)

        missing_src = queries.get_missing_source_deals()
        if not missing_src.empty:
            st.markdown(f"**{len(missing_src)} real deals missing source URLs:**")
            st.dataframe(missing_src, use_container_width=True)
