"""
M&A Database Dashboard — Bloomberg dark mode, 6 tabs, global sidebar filters.
Consistent with lbo-engine and pe-target-screener visual style.
All charts use Plotly dark template. No 3D, no decorative elements.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="M&A Database",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Bloomberg dark CSS
st.markdown("""
<style>
    .main { background-color: #0A0A0A; }
    .stApp { background-color: #0A0A0A; color: #E8E8E8; }
    .metric-card {
        background: #1A1A2E; border: 1px solid #00D4AA;
        border-radius: 8px; padding: 16px; margin: 4px;
    }
    .metric-value { font-size: 28px; font-weight: 700; color: #00D4AA; }
    .metric-label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; }
    .snapshot-box {
        background: #111; border-left: 3px solid #00D4AA;
        padding: 16px; border-radius: 4px; margin: 12px 0;
    }
    .synthetic-badge {
        background: #3D2B1F; color: #F0B27A; padding: 2px 8px;
        border-radius: 4px; font-size: 11px;
    }
    h1, h2, h3 { color: #E8E8E8; }
    .stSelectbox, .stMultiSelect { color: #E8E8E8; }
    /* Force transparent backgrounds on all Plotly charts */
    .js-plotly-plot .plotly .main-svg { background: transparent !important; }
    .js-plotly-plot .plotly .bg { fill: transparent !important; }
</style>
""", unsafe_allow_html=True)

CONFIG = load_config("config.yaml")
TEMPLATE = CONFIG["dashboard"]["plotly_template"]
COLORS = CONFIG["dashboard"]["chart_color_palette"]
H = CONFIG["dashboard"]["chart_height"]

# ---------------------------------------------------------------------------
# Semantic color system — consistent across all signals, regimes, quality tiers
# ---------------------------------------------------------------------------
MIN_SAMPLE = 3  # Minimum deal count for a credible signal/classification
DARK_BG = dict(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

SIGNAL_COLORS = {
    "Overheating":       "#FF6B6B",
    "Narrowing":         "#F0B27A",
    "Healthy Growth":    "#00D4AA",
    "Cooling":           "#45B7D1",
    "Insufficient Data": "#888888",
}
REGIME_COLORS = {
    "Peak / Late-Cycle":      "#FF6B6B",
    "Recovery / Opportunity": "#00D4AA",
    "Selective / Cautious":   "#F0B27A",
    "Trough / Distressed":    "#45B7D1",
    "Indeterminate":          "#888888",
}
COMPLETENESS_COLORS = {"High": "#00D4AA", "Medium": "#F7DC6F", "Low": "#FF6B6B"}
CONFIDENCE_COLORS = {
    "high confidence":    "#00D4AA",
    "moderate confidence":"#F7DC6F",
    "low confidence":     "#F0B27A",
    "insufficient data":  "#888888",
}
STANCE_COLORS = {
    "Premium Buyer":               "#FF6B6B",
    "Value Buyer":                 "#00D4AA",
    "Market Buyer":                "#888888",
    "Unknown":                     "#888888",
    "Insufficient valuation data": "#888888",
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
# DB init (cached)
# ---------------------------------------------------------------------------
@st.cache_resource
def _init():
    init_db(CONFIG["database"]["path"])
    create_schema()
    if queries.get_deals_count() == 0:
        seed_real_deals(CONFIG)
        cnt = queries.get_deals_count()
        seed_synthetic_deals(CONFIG, cnt)
    return True

_init()


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
- DuckDB columnar engine — optimized for analytical aggregation

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

    st.sidebar.markdown("## Filters")

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
st.markdown("# 🏦 M&A Database + Analysis Tool")
st.markdown("Institutional M&A intelligence: valuations, sponsor behavior, and market activity trends.")
st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tabs = st.tabs(["Overview", "Valuation", "Market Activity", "Sponsor Intelligence", "Deal Explorer", "Data Management"])

# ===========================================================================
# TAB 1: OVERVIEW
# ===========================================================================
with tabs[0]:
    kpis = _get_kpi_summary(_filter_key(filters))

    if not kpis:
        st.warning("No deals match the current filters.")
    else:
        # KPI row — two rows of 4
        def kpi(col, label, value):
            col.markdown(f"""<div class="metric-card">
                <div class="metric-value">{value}</div>
                <div class="metric-label">{label}</div>
            </div>""", unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        kpi(c1, "Total Deals", f"{kpis.get('total_deals', 0):,}")
        kpi(c2, "Total Deal Value", fmt_currency(kpis.get("total_deal_value_usd"), decimals=1, suffix="B"))
        total = kpis.get("total_deals", 1) or 1
        sp_pct = kpis.get("sponsor_deal_count", 0) / total * 100
        st_pct = kpis.get("strategic_deal_count", 0) / total * 100
        kpi(c3, "Sponsor / Strategic Split", f"{sp_pct:.0f}% / {st_pct:.0f}%")
        kpi(c4, "Median EV/EBITDA", fmt_multiple(kpis.get("median_ev_to_ebitda")))

        c5, c6, c7, c8 = st.columns(4)
        kpi(c5, "Sponsor Deals", f"{kpis.get('sponsor_deal_count', 0):,}")
        kpi(c6, "Strategic Deals", f"{kpis.get('strategic_deal_count', 0):,}")
        kpi(c7, "Most Active Sector", kpis.get("most_active_sector", "N/A"))
        kpi(c8, "Most Active Sponsor", kpis.get("most_active_sponsor", "N/A"))

        # Data Composition block — makes real/synthetic separation immediately visible
        st.markdown("---")
        st.markdown("#### Data Composition")
        _real_n = kpis.get("real_deals", 0)
        _synth_n = kpis.get("synthetic_deals", 0)
        _total_n = kpis.get("total_deals", 1) or 1
        _real_pct = _real_n / _total_n * 100
        _synth_pct = _synth_n / _total_n * 100

        dc1, dc2, dc3 = st.columns(3)
        dc1.metric("🟢 Real Deals", f"{_real_n:,}", f"{_real_pct:.0f}% of universe")
        dc2.metric("🔵 Synthetic Deals", f"{_synth_n:,}", f"{_synth_pct:.0f}% of universe")
        dc3.metric("⚪ Combined Universe", f"{_total_n:,}", "All deal records")
        st.caption(
            "Data origin is controlled via the sidebar filter. "
            "Analytics default to **real transactions only** — use 'all' to include the synthetic extension layer."
        )

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            # Deal count over time
            cnt_df = queries.get_deal_count_by_year(filters)
            if not cnt_df.empty:
                fig = px.line(cnt_df, x="year", y="deal_count",
                              title="Deal Count Over Time",
                              template=TEMPLATE, color_discrete_sequence=COLORS)
                fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deals", **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Deal type distribution
            dt_df = queries.get_deal_type_distribution(filters)
            if not dt_df.empty:
                fig = px.pie(dt_df, names="deal_type", values="deal_count",
                             title="Deal Type Distribution",
                             template=TEMPLATE, color_discrete_sequence=COLORS, hole=0.4)
                fig.update_layout(height=H, **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            # Top sectors
            sec_df = queries.get_deal_count_by_sector(filters, top_n=10)
            if not sec_df.empty:
                fig = px.bar(sec_df.sort_values("deal_count"), x="deal_count", y="sector_name",
                             orientation="h", title="Top 10 Sectors by Deal Count",
                             template=TEMPLATE, color_discrete_sequence=COLORS)
                fig.update_layout(height=H, xaxis_title="Deals", yaxis_title="", **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)

        with col4:
            # Top acquirers
            acq_df = queries.get_deal_count_by_acquirer(filters, top_n=10)
            if not acq_df.empty:
                fig = px.bar(acq_df.sort_values("deal_count"), x="deal_count", y="acquirer_name",
                             orientation="h", title="Top 10 Acquirers by Deal Count",
                             color="acquirer_type",
                             template=TEMPLATE, color_discrete_sequence=COLORS)
                fig.update_layout(height=H, xaxis_title="Deals", yaxis_title="", **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)

        # Market Regime Section
        st.markdown("### Market Regime")
        regime_df = regime_mod.classify_regimes(filters)
        current_regime = regime_mod.get_current_regime(filters)

        if not regime_df.empty:
            regime_df["color"] = regime_df["regime_label"].map(REGIME_COLORS).fillna("#888888")

            col_regime1, col_regime2 = st.columns([2, 1])
            with col_regime1:
                # Regime timeline: colored bar chart
                fig_regime = px.bar(
                    regime_df, x="year", y="deal_count",
                    color="regime_label",
                    color_discrete_map=REGIME_COLORS,
                    title="Market Regime Timeline (Annual Deal Activity, colored by Regime)",
                    template=TEMPLATE,
                    labels={"deal_count": "Deal Count", "year": "Year", "regime_label": "Regime"},
                )
                fig_regime.update_layout(height=380, legend_title="Regime", **DARK_BG)
                st.plotly_chart(fig_regime, use_container_width=True)

            with col_regime2:
                if current_regime:
                    label = current_regime.get("regime_label", "N/A")
                    year = current_regime.get("year", "N/A")
                    explanation = current_regime.get("explanation", "")
                    color = REGIME_COLORS.get(label, "#888888")
                    st.markdown(f"""
                    <div style="background:#1A1A2E; border-left: 4px solid {color};
                         padding:16px; border-radius:4px; margin-top:8px;">
                        <div style="font-size:11px; color:#888; text-transform:uppercase; letter-spacing:1px;">
                            Current Regime ({year})</div>
                        <div style="font-size:22px; font-weight:700; color:{color}; margin:8px 0;">
                            {label}</div>
                        <div style="font-size:13px; color:#CCC; line-height:1.5;">
                            {explanation}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    transition = regime_mod.regime_transition_summary(filters)
                    if transition:
                        st.markdown(f"<div style='margin-top:12px; font-size:13px; color:#AAA;'>{transition}</div>",
                                    unsafe_allow_html=True)

        st.markdown("---")

        # M&A Market Snapshot
        st.markdown("### M&A Market Snapshot")
        with st.expander("View auto-generated analyst memo", expanded=True):
            memo = snapshot.generate_snapshot(filters, CONFIG)
            st.markdown(f'<div class="snapshot-box">{memo}</div>', unsafe_allow_html=True)

        # Real vs synthetic composition callout
        audit_df = queries.get_data_origin_audit()
        if not audit_df.empty:
            audit_map = {r["data_origin"]: r["deal_count"] for r in audit_df.to_dict("records")}
            real_n = audit_map.get("real", 0)
            syn_n = audit_map.get("synthetic", 0)
            st.caption(f"Data composition: {real_n} verified real deals · {syn_n} synthetic extension · "
                       f"Use the **Data Origin** filter to view real-only.")


# ===========================================================================
# TAB 2: VALUATION
# ===========================================================================
with tabs[1]:
    st.markdown("### Valuation Analysis")
    st.caption("Sector multiples, premium dynamics, and valuation regime shifts.")

    # EV/EBITDA box plot — filter sectors with < MIN_SAMPLE valid observations
    ev_df = valuation.ev_ebitda_by_sector(filters)
    if not ev_df.empty:
        _ev_counts = ev_df.groupby("sector_name")["ev_to_ebitda"].count()
        _ev_included = _ev_counts[_ev_counts >= MIN_SAMPLE].index.tolist()
        _ev_excluded = _ev_counts[_ev_counts < MIN_SAMPLE].index.tolist()
        _ev_caution = _ev_counts[(_ev_counts >= MIN_SAMPLE) & (_ev_counts <= 5)].index.tolist()
        ev_df_f = ev_df[ev_df["sector_name"].isin(_ev_included)]
        if not ev_df_f.empty:
            fig = px.box(ev_df_f, x="sector_name", y="ev_to_ebitda",
                         title="EV/EBITDA Distribution by Sector",
                         template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, xaxis_title="Sector", yaxis_title="EV/EBITDA (x)",
                              xaxis_tickangle=-30, **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)
            captions = []
            if _ev_excluded:
                captions.append(f"Excluded (< {MIN_SAMPLE} obs): {', '.join(_ev_excluded)}.")
            if _ev_caution:
                captions.append(f"Interpret with caution (3–5 obs): {', '.join(_ev_caution)}.")
            if captions:
                st.caption(" ".join(captions))

    col1, col2 = st.columns(2)

    with col1:
        # EV/Revenue box plot — same MIN_SAMPLE filter
        rev_df = valuation.ev_revenue_by_sector(filters)
        if not rev_df.empty:
            _rev_counts = rev_df.groupby("sector_name")["ev_to_revenue"].count()
            _rev_included = _rev_counts[_rev_counts >= MIN_SAMPLE].index.tolist()
            _rev_excluded = _rev_counts[_rev_counts < MIN_SAMPLE].index.tolist()
            rev_df_f = rev_df[rev_df["sector_name"].isin(_rev_included)]
            if not rev_df_f.empty:
                fig = px.box(rev_df_f, x="sector_name", y="ev_to_revenue",
                             title="EV/Revenue Distribution by Sector",
                             template=TEMPLATE, color_discrete_sequence=COLORS)
                fig.update_layout(height=H, xaxis_tickangle=-30, xaxis_title="Sector",
                                  yaxis_title="EV/Revenue (x)", **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)
                if _rev_excluded:
                    st.caption(f"Excluded (< {MIN_SAMPLE} obs): {', '.join(_rev_excluded)}.")

    with col2:
        # Premium paid histogram
        prem_df = valuation.premium_distribution(filters)
        if not prem_df.empty:
            fig = px.histogram(prem_df, x="premium_paid_pct",
                               title="Premium Paid % Distribution (Public Targets)",
                               template=TEMPLATE, color_discrete_sequence=COLORS,
                               nbins=30)
            fig.update_layout(height=H, xaxis_title="Premium Paid (%)", yaxis_title="Count",
                              **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    # Median EV/EBITDA over time (valuation regime)
    regime_df = valuation.median_ev_ebitda_by_sector_year(filters)
    if not regime_df.empty:
        fig = px.line(regime_df, x="year", y="median_ev_to_ebitda", color="sector_name",
                      title="Median EV/EBITDA Over Time by Sector (Valuation Regime Shifts)",
                      template=TEMPLATE, color_discrete_sequence=COLORS)
        fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Median EV/EBITDA (x)",
                          **DARK_BG)
        st.plotly_chart(fig, use_container_width=True)

    # Sponsor vs strategic multiples
    spvs = valuation.sponsor_vs_strategic_multiples(filters)
    if not spvs.empty:
        fig = px.bar(spvs, x="acquirer_type", y=["avg_ev_to_ebitda", "median_ev_to_ebitda"],
                     barmode="group",
                     title="Sponsor vs. Strategic: Entry EV/EBITDA Comparison",
                     template=TEMPLATE, color_discrete_sequence=COLORS)
        fig.update_layout(height=400, xaxis_title="Acquirer Type", yaxis_title="EV/EBITDA (x)",
                          **DARK_BG)
        st.plotly_chart(fig, use_container_width=True)

    # Valuation stats table
    stats = valuation.sector_valuation_stats(filters)
    if not stats.empty:
        st.markdown("#### EV/EBITDA Summary Statistics by Sector")
        st.dataframe(stats.style.format({
            "median": "{:.1f}x", "mean": "{:.1f}x", "p25": "{:.1f}x", "p75": "{:.1f}x"
        }), use_container_width=True)

    # --- Relative Valuation Section ---
    st.markdown("---")
    st.markdown("### Relative Valuation Analysis")
    st.caption("Sector premium/discount vs market median, historical percentile positioning, and sponsor vs strategic spread.")

    rel_df = rel_val_mod.sector_relative_valuation(filters)

    if not rel_df.empty:
        # rel_df is already filtered to >= MIN_SAMPLE in relative_valuation.py
        col_rel1, col_rel2 = st.columns(2)

        with col_rel1:
            # Horizontal bar: sector premium/discount vs market
            fig_rel = px.bar(
                rel_df.sort_values("premium_discount"),
                x="premium_discount", y="sector_name",
                orientation="h",
                color="premium_discount",
                color_continuous_scale=["#FF6B6B", "#444", "#00D4AA"],
                color_continuous_midpoint=0,
                title="Sector EV/EBITDA Premium / Discount vs Market",
                template=TEMPLATE,
                labels={"premium_discount": "Premium/Discount (x)", "sector_name": ""},
                text="premium_discount",
            )
            fig_rel.update_traces(texttemplate="%{text:+.1f}x", textposition="outside")
            fig_rel.update_layout(height=H, coloraxis_showscale=False,
                                  xaxis_title="vs Market Median (x)", **DARK_BG)
            st.plotly_chart(fig_rel, use_container_width=True)
            st.caption(f"Only sectors with ≥ {MIN_SAMPLE} valid EV/EBITDA observations shown.")

        with col_rel2:
            # Historical percentile bar
            rel_pct_df = rel_df.dropna(subset=["historical_percentile"]).sort_values("historical_percentile", ascending=False)
            if not rel_pct_df.empty:
                fig_pct = px.bar(
                    rel_pct_df,
                    x="historical_percentile", y="sector_name",
                    orientation="h",
                    color="historical_percentile",
                    color_continuous_scale=["#00D4AA", "#F7DC6F", "#FF6B6B"],
                    color_continuous_midpoint=50,
                    title="Sector EV/EBITDA Historical Percentile",
                    template=TEMPLATE,
                    labels={"historical_percentile": "Percentile", "sector_name": ""},
                    text="historical_percentile",
                )
                fig_pct.update_traces(texttemplate="%{text:.0f}th", textposition="outside")
                fig_pct.update_layout(height=H, coloraxis_showscale=False,
                                      xaxis_title="Historical Percentile (0–100th)", **DARK_BG)
                st.plotly_chart(fig_pct, use_container_width=True)
                st.caption(f"Only sectors with ≥ {MIN_SAMPLE} valid observations shown.")

        # Sponsor vs strategic spread by sector
        sv_df = rel_val_mod.sponsor_vs_strategic_premium(filters)
        if not sv_df.empty:
            fig_sv = px.bar(
                sv_df.sort_values("spread"),
                x="spread", y="sector_name",
                orientation="h",
                color="spread",
                color_continuous_scale=["#45B7D1", "#444", "#00D4AA"],
                color_continuous_midpoint=0,
                title="Sponsor vs. Strategic Entry Premium by Sector (Spread in EV/EBITDA x)",
                template=TEMPLATE,
                labels={"spread": "Sponsor − Strategic (x)", "sector_name": ""},
                text="spread",
            )
            fig_sv.update_traces(texttemplate="%{text:+.1f}x", textposition="outside")
            fig_sv.update_layout(height=H, coloraxis_showscale=False, **DARK_BG)
            st.plotly_chart(fig_sv, use_container_width=True)
            st.caption(f"Only sectors where both sponsor and strategic have ≥ {MIN_SAMPLE} deals with valid EV/EBITDA.")

        # Relative valuation narrative
        narrative = rel_val_mod.relative_valuation_narrative(filters)
        if narrative:
            st.markdown(f'<div class="snapshot-box" style="border-left-color: #45B7D1;">{narrative}</div>',
                        unsafe_allow_html=True)

        # Full relative valuation table
        with st.expander("Relative Valuation Detail Table"):
            display_rel = rel_df[["sector_name", "deal_count", "sector_median",
                                   "market_median", "premium_discount",
                                   "historical_percentile", "interpretation_label"]].copy()
            st.dataframe(display_rel.style.format({
                "sector_median": "{:.1f}x",
                "market_median": "{:.1f}x",
                "premium_discount": "{:+.1f}x",
                "historical_percentile": "{:.0f}th",
            }, na_rep="N/A"), use_container_width=True)


# ===========================================================================
# TAB 3: MARKET ACTIVITY
# ===========================================================================
with tabs[2]:
    st.markdown("### Market Activity")
    st.caption("Volume trends, sector heatmaps, sponsor vs strategic mix.")

    col1, col2 = st.columns(2)

    with col1:
        cnt_df = market_activity.deal_count_over_time(filters)
        if not cnt_df.empty:
            fig = px.area(cnt_df, x="year", y="deal_count",
                          title="Annual Deal Count",
                          template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        val_df = market_activity.deal_value_over_time(filters)
        if not val_df.empty:
            val_df["total_value_B"] = val_df["total_value_usd"] / 1000
            fig = px.area(val_df, x="year", y="total_value_B",
                          title="Annual Deal Value ($B)",
                          template=TEMPLATE, color_discrete_sequence=[COLORS[1]])
            fig.update_layout(height=H, yaxis_title="Deal Value ($B)", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    # Sector heatmap
    heatmap_df = market_activity.sector_activity_heatmap(filters)
    if not heatmap_df.empty and "sector_name" in heatmap_df.columns:
        year_cols = [c for c in heatmap_df.columns if str(c).isdigit() or (isinstance(c, (int, float)))]
        if year_cols:
            z = heatmap_df[year_cols].values
            fig = go.Figure(data=go.Heatmap(
                z=z,
                x=[str(int(c)) for c in year_cols],
                y=heatmap_df["sector_name"].tolist(),
                colorscale="Teal",
                showscale=True,
            ))
            fig.update_layout(title="Sector Activity Heatmap (Deal Count by Year)",
                              template=TEMPLATE, height=H + 100,
                              xaxis_title="Year", yaxis_title="Sector", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Sponsor vs strategic trend
        svs_df = market_activity.sponsor_vs_strategic_trend(filters)
        if not svs_df.empty:
            fig = px.bar(svs_df, x="year", y="deal_count", color="acquirer_type",
                         barmode="stack",
                         title="Sponsor vs. Strategic Activity Over Time",
                         template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, yaxis_title="Deal Count", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Deal status breakdown
        status_df = market_activity.deal_status_breakdown(filters)
        if not status_df.empty:
            fig = px.bar(status_df, x="year", y="deal_count", color="deal_status",
                         barmode="stack",
                         title="Deal Status Breakdown Over Time",
                         template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, yaxis_title="Deal Count", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    # Sector value treemap
    tree_df = market_activity.sector_value_treemap(filters)
    if not tree_df.empty and "total_value_usd" in tree_df.columns:
        fig = px.treemap(tree_df, path=["sector_name"], values="total_value_usd",
                         title="Deal Value by Sector (Treemap)",
                         template=TEMPLATE, color_discrete_sequence=COLORS)
        fig.update_layout(height=500, **DARK_BG)
        st.plotly_chart(fig, use_container_width=True)

    # --- Market Signals Section ---
    st.markdown("---")
    st.markdown("### Market Signals — Sector Imbalance Detection")
    st.caption(
        "Activity momentum vs valuation momentum by sector. "
        "Each sector is classified into one of four quadrants: "
        "Overheating, Healthy Growth, Narrowing, or Cooling."
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
                title="Sector Quadrant Chart: Activity Momentum vs Valuation Momentum",
                template=TEMPLATE,
                labels={
                    "activity_momentum_pct": "Activity Momentum (% change in deal count)",
                    "valuation_momentum_pct": "Valuation Momentum (% change in EV/EBITDA)",
                },
            )
            fig_scatter.update_traces(textposition="top center", marker=dict(size=14))
            fig_scatter.add_hline(y=0, line_dash="dash", line_color="#555", line_width=1)
            fig_scatter.add_vline(x=0, line_dash="dash", line_color="#555", line_width=1)
            fig_scatter.update_layout(height=520, **DARK_BG)
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
            return f"color: {SIGNAL_COLORS.get(val, '#888888')}"

        def _color_confidence(val):
            return f"color: {CONFIDENCE_COLORS.get(val, '#888888')}"

        styled = (
            display_imbalance.style
            .applymap(_color_signal, subset=["Signal"])
            .applymap(_color_confidence, subset=["Confidence"])
            .format({
                "Activity Momentum (%)": "{:+.1f}%",
                "Valuation Momentum (%)": "{:+.1f}%",
                "Recent Median EV/EBITDA": "{:.1f}x",
            }, na_rep="N/A")
        )
        st.dataframe(styled, use_container_width=True)
        st.caption(f"'n (recent deals)' = deal count in the most recent 2-year window. Signals with n < {MIN_SAMPLE} are classified as Insufficient Data.")

        # Narrative
        narrative = imbalance_mod.imbalance_narrative(filters)
        if narrative:
            st.markdown("#### Sector Signal Narrative")
            st.markdown(f'<div class="snapshot-box" style="border-left-color: {SIGNAL_COLORS["Narrowing"]};">{narrative}</div>',
                        unsafe_allow_html=True)
    else:
        st.info("Sector imbalance analysis requires 5+ years of data. Expand the date range to enable this analysis.")


# ===========================================================================
# TAB 4: SPONSOR INTELLIGENCE
# ===========================================================================
with tabs[3]:
    st.markdown("### Sponsor Intelligence")
    st.caption("PE sponsor rankings, sector preferences, and entry multiples.")

    rankings = sponsor_intel.sponsor_rankings(filters, top_n=15)

    col1, col2 = st.columns(2)

    with col1:
        if not rankings.empty:
            fig = px.bar(rankings.sort_values("deal_count"), x="deal_count", y="sponsor_name",
                         orientation="h", title="Top Sponsors by Deal Count",
                         template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, xaxis_title="Deals", yaxis_title="", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_value = sponsor_intel.top_sponsors_by_value(filters, top_n=15)
        if not top_value.empty:
            top_value["total_B"] = top_value["total_deal_value_usd"] / 1000
            fig = px.bar(top_value.sort_values("total_B"), x="total_B", y="sponsor_name",
                         orientation="h", title="Top Sponsors by Total Deal Value ($B)",
                         template=TEMPLATE, color_discrete_sequence=[COLORS[1]])
            fig.update_layout(height=H, xaxis_title="Total Deal Value ($B)", yaxis_title="", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    # Sponsor sector heatmap
    sec_heat = sponsor_intel.sponsor_sector_heatmap(filters, top_n_sponsors=12)
    if not sec_heat.empty and "sponsor_name" in sec_heat.columns:
        sector_cols = [c for c in sec_heat.columns if c != "sponsor_name"]
        if sector_cols:
            fig = go.Figure(data=go.Heatmap(
                z=sec_heat[sector_cols].values,
                x=sector_cols,
                y=sec_heat["sponsor_name"].tolist(),
                colorscale="Teal",
            ))
            fig.update_layout(title="Sponsor Sector Preferences (Deal Count Heatmap)",
                              template=TEMPLATE, height=H + 100,
                              xaxis_tickangle=-30, **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Entry multiples — only sponsors with >= MIN_SAMPLE deals with valid EV/EBITDA
        entry_df = sponsor_intel.sponsor_entry_multiples(filters, top_n=15)
        if not entry_df.empty:
            # filter to sponsors with enough valuation data (ev_count column if present, else deal_count proxy)
            _ev_count_col = "ev_count" if "ev_count" in entry_df.columns else None
            if _ev_count_col:
                _entry_incl = entry_df[entry_df[_ev_count_col] >= MIN_SAMPLE]
                _entry_excl = entry_df[entry_df[_ev_count_col] < MIN_SAMPLE]["sponsor_name"].tolist()
            else:
                _entry_incl = entry_df
                _entry_excl = []
            if not _entry_incl.empty:
                fig = px.bar(_entry_incl.sort_values("avg_ev_to_ebitda"), x="avg_ev_to_ebitda", y="sponsor_name",
                             orientation="h", title="Average Entry EV/EBITDA by Sponsor",
                             template=TEMPLATE, color_discrete_sequence=[COLORS[4]])
                fig.update_layout(height=H, xaxis_title="Avg EV/EBITDA (x)", yaxis_title="", **DARK_BG)
                st.plotly_chart(fig, use_container_width=True)
                if _entry_excl:
                    st.caption(f"Excluded (< {MIN_SAMPLE} deals with valid EV/EBITDA): {', '.join(_entry_excl)}.")

    with col4:
        # Sponsor deal trend
        trend_df = sponsor_intel.sponsor_deal_trend(filters, top_n_sponsors=5)
        if not trend_df.empty:
            fig = px.line(trend_df, x="year", y="deal_count", color="sponsor_name",
                          title="Top Sponsors: Annual Deal Count",
                          template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=H, xaxis_title="Year", yaxis_title="Deal Count", **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

    # Rankings table — convert $M columns to $B for readability
    if not rankings.empty:
        st.markdown("#### Sponsor Rankings Table")
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

    # --- Sponsor Profiles Section ---
    st.markdown("---")
    st.markdown("### Sponsor Behavioral Profiles")
    st.caption("Valuation stance, sector concentration, and deal size profile for each sponsor with 3+ deals.")

    all_profiles_df = sponsor_profile_mod.generate_all_profiles(filters, min_deals=3)

    if not all_profiles_df.empty:
        # Summary profiles table
        def _color_stance(val):
            return f"color: {STANCE_COLORS.get(val, '#888888')}"

        display_profiles = all_profiles_df.copy()
        fmt_cols = {"avg_ev_ebitda": "{:.1f}x", "ev_premium_vs_market": "{:+.1f}x"}
        style_cols = ["valuation_stance"]
        # Include ev_ebitda_count column if present
        st.dataframe(
            display_profiles.style
            .applymap(_color_stance, subset=style_cols)
            .format(fmt_cols, na_rep="N/A"),
            use_container_width=True,
        )
        st.caption(f"Valuation stance requires ≥ {MIN_SAMPLE} deals with valid EV/EBITDA data. 'Insufficient valuation data' shown otherwise.")

        # Drill-down: profile card for selected sponsor
        st.markdown("#### Sponsor Profile Card")
        eligible_sponsors = all_profiles_df["sponsor_name"].tolist()
        selected_sponsor = st.selectbox("Select a sponsor for detailed profile", eligible_sponsors)

        if selected_sponsor:
            profile = sponsor_profile_mod.generate_sponsor_profile(selected_sponsor, filters)
            if profile:
                stance = profile.get("valuation_stance", "Unknown")
                stance_color = STANCE_COLORS.get(stance, "#888888")
                narrative = profile.get("narrative", "")
                sectors = ", ".join(profile.get("preferred_sectors", [])[:3]) or "N/A"
                ev = profile.get("avg_ev_ebitda")
                premium = profile.get("ev_premium_vs_market")
                deal_size = profile.get("avg_deal_size_usd")
                size_stance = profile.get("deal_size_stance", "N/A")
                ev_cnt = profile.get("ev_ebitda_count", 0)
                deal_cnt = profile.get("deal_count", 0)

                cp1, cp2, cp3, cp4 = st.columns(4)
                cp1.markdown(f"""<div class="metric-card">
                    <div class="metric-value" style="color:{stance_color}; font-size:20px;">{stance}</div>
                    <div class="metric-label">Valuation Stance</div>
                </div>""", unsafe_allow_html=True)
                cp2.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{f'{ev:.1f}x' if ev else 'N/A'}</div>
                    <div class="metric-label">Avg Entry EV/EBITDA</div>
                </div>""", unsafe_allow_html=True)
                cp3.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{f'{premium:+.1f}x' if premium is not None else 'N/A'}</div>
                    <div class="metric-label">vs Market Median</div>
                </div>""", unsafe_allow_html=True)
                cp4.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{f'${deal_size/1000:.1f}B' if deal_size else 'N/A'}</div>
                    <div class="metric-label">Avg Deal Size</div>
                </div>""", unsafe_allow_html=True)

                # Confidence display for valuation stance
                ev_badge = confidence_badge(ev_cnt)
                stance_note = (
                    f"Valuation classification based on {ev_cnt}/{deal_cnt} deals with valid EV/EBITDA data. "
                    + ev_badge
                )
                st.markdown(stance_note, unsafe_allow_html=True)
                st.markdown(f"**Preferred Sectors:** {sectors} &nbsp;&nbsp; **Deal Size Profile:** {size_stance}",
                            unsafe_allow_html=True)
                st.markdown(f'<div class="snapshot-box" style="border-left-color: {stance_color};">{narrative}</div>',
                            unsafe_allow_html=True)
    else:
        st.info("No sponsors with 3+ deals found under current filters.")


# ===========================================================================
# TAB 5: DEAL EXPLORER
# ===========================================================================
with tabs[4]:
    st.markdown("### Deal Explorer")
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
                return ["background-color: #1A120B"] * len(row)
            return [""] * len(row)

        st.dataframe(
            display_df.style.apply(highlight_synthetic, axis=1).format({
                "Deal Value ($M)": "${:,.0f}M",
                "EV/EBITDA": "{:.1f}x",
                "Completeness": "{:.0f}%",
            }, na_rep="N/A"),
            use_container_width=True,
            height=500,
        )

        # Deal detail expander
        st.markdown("#### Deal Detail")
        deal_ids = deals_df["deal_id"].tolist()
        target_names = deals_df["target_name"].tolist()
        options = [f"{t} ({d[:8]}...)" for t, d in zip(target_names, deal_ids)]
        selected_idx = st.selectbox("Select a deal to expand", range(len(options)),
                                    format_func=lambda i: options[i])

        if selected_idx is not None:
            row = deals_df.iloc[selected_idx]
            is_synthetic = row.get("data_origin") == "synthetic"
            if is_synthetic:
                st.warning("⚠️ SYNTHETIC RECORD — This deal is simulated and does not represent a real transaction.")
            with st.expander(f"Deal Detail: {row.get('target_name')}", expanded=True):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown(f"**Target:** {row.get('target_name')}")
                    st.markdown(f"**Acquirer:** {row.get('acquirer_name', 'N/A')}")
                    st.markdown(f"**Type:** {row.get('deal_type')}")
                    st.markdown(f"**Status:** {row.get('deal_status')}")
                with c2:
                    st.markdown(f"**Deal Value:** {fmt_currency(row.get('deal_value_usd'))}")
                    st.markdown(f"**EV/EBITDA:** {fmt_multiple(row.get('ev_to_ebitda'))}")
                    st.markdown(f"**EV/Revenue:** {fmt_multiple(row.get('ev_to_revenue'))}")
                    st.markdown(f"**Premium Paid:** {fmt_pct(row.get('premium_paid_pct'))}")
                with c3:
                    st.markdown(f"**Sector:** {row.get('sector_name', 'N/A')}")
                    st.markdown(f"**Announced:** {row.get('announcement_date')}")
                    st.markdown(f"**Closed:** {row.get('closing_date', 'N/A')}")
                    st.markdown(f"**Completeness:** {fmt_pct(row.get('completeness_score'))}")

        # Export buttons
        st.markdown("#### Export")
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
    st.markdown("### Data Management")

    # System Architecture card
    st.markdown("#### System Architecture")
    _CARD = "background:#111; border:1px solid #00D4AA33; border-radius:6px; padding:14px; height:160px;"
    _HDR = "font-size:11px; font-weight:700; color:#00D4AA; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;"
    _LI = "font-size:12px; color:#BBB; line-height:1.7;"

    # Pipeline flow
    st.markdown("""
<div style="text-align:center; padding:10px 0; font-size:13px; color:#888; letter-spacing:2px;">
  <span style="color:#00D4AA; font-weight:700;">INGEST</span>
  &nbsp;→&nbsp;
  <span style="color:#45B7D1; font-weight:700;">VALIDATE</span>
  &nbsp;→&nbsp;
  <span style="color:#F7DC6F; font-weight:700;">SCORE</span>
  &nbsp;→&nbsp;
  <span style="color:#BB8FCE; font-weight:700;">STORE</span>
  &nbsp;→&nbsp;
  <span style="color:#00D4AA; font-weight:700;">ANALYZE</span>
  &nbsp;→&nbsp;
  <span style="color:#F0B27A; font-weight:700;">EXPORT</span>
</div>
""", unsafe_allow_html=True)

    arch1, arch2, arch3 = st.columns(3)
    with arch1:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Ingestion</div>
  <div style="{_LI}">
    · Real deal seed loader<br>
    · Synthetic deal generator<br>
    · CSV bulk import<br>
    · Form-based manual entry
  </div>
</div>""", unsafe_allow_html=True)

    with arch2:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Validation</div>
  <div style="{_LI}">
    · Required field checks<br>
    · Type &amp; range validation<br>
    · Duplicate detection<br>
    · Future-date guard
  </div>
</div>""", unsafe_allow_html=True)

    with arch3:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Scoring</div>
  <div style="{_LI}">
    · Weighted completeness<br>
    · Rule-based confidence<br>
    · Quality tier assignment<br>
    · Low-completeness cap
  </div>
</div>""", unsafe_allow_html=True)

    arch4, arch5, arch6 = st.columns(3)
    with arch4:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Storage</div>
  <div style="{_LI}">
    · DuckDB columnar engine<br>
    · 5 normalized tables<br>
    · 2 analytical views<br>
    · Single-file deployment
  </div>
</div>""", unsafe_allow_html=True)

    with arch5:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Analytics</div>
  <div style="{_LI}">
    · Valuation &amp; activity trends<br>
    · Regime &amp; imbalance detection<br>
    · Sponsor profiling<br>
    · Relative valuation
  </div>
</div>""", unsafe_allow_html=True)

    with arch6:
        st.markdown(f"""
<div style="{_CARD}">
  <div style="{_HDR}">Export</div>
  <div style="{_LI}">
    · Filtered CSV download<br>
    · Formatted Excel (multi-sheet)<br>
    · PDF report (Phase 2)<br>
    · API connectors (future)
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("---")

    mgmt_tabs = st.tabs(["Add Deal", "Bulk CSV Import", "Completeness Audit", "Data Origin Audit"])

    # --- Add Deal Form ---
    with mgmt_tabs[0]:
        st.markdown("#### Add New Deal")
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

    # --- Bulk CSV Import ---
    with mgmt_tabs[1]:
        st.markdown("#### Bulk CSV Import")
        st.caption("Upload a CSV with deal records. Required columns: target_name, announcement_date, deal_type, deal_status, data_origin")
        uploaded = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded:
            import tempfile
            with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
                f.write(uploaded.read())
                tmpfile = f.name

            preview = preview_csv(tmpfile, CONFIG)
            st.markdown(f"**{preview['total_rows']} rows** | **{preview['valid_rows']} valid** | **{len(preview['invalid_rows'])} invalid** | **{len(preview['duplicate_warnings'])} duplicate warnings**")

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

    # --- Completeness Audit ---
    with mgmt_tabs[2]:
        st.markdown("#### Completeness Audit")

        with st.expander("Scoring Methodology", expanded=False):
            st.markdown("""
**Completeness Scoring Methodology**

Each deal is scored on a weighted scale based on field importance for M&A analysis:

- **Tier 1 (weight 3.0)** — Deal identity: announcement date, acquirer, target, deal type, sector, deal value, status
- **Tier 2 (weight 2.0)** — Analytical value: EV/EBITDA, EV/Revenue, premium paid, enterprise value, target EBITDA, target revenue, closing date, geography
- **Tier 3 (weight 1.0)** — Secondary: financing structure, leverage, hostile/friendly, notes, source URL, sub-industry

**Quality tiers:** High (≥80%) · Medium (50–79%) · Low (<50%)

**Confidence** is scored separately — based on data origin (real vs synthetic) and source verification.
Low-completeness records (below 30%) are capped at 0.3 confidence regardless of source quality.

Low-quality records can be filtered out using the **Completeness Threshold** slider in the sidebar.
""")

        comp_df = queries.get_completeness_distribution(filters)
        if comp_df.empty:
            st.info("No data.")
        else:
            fig = px.histogram(comp_df, x="completeness_score", nbins=20,
                               title="Completeness Score Distribution",
                               template=TEMPLATE, color_discrete_sequence=COLORS)
            fig.update_layout(height=350, xaxis_title="Completeness Score (%)", yaxis_title="Count",
                              **DARK_BG)
            st.plotly_chart(fig, use_container_width=True)

            low_q = comp_df[comp_df["completeness_score"] < 50]
            st.markdown(f"**{len(low_q)} low-quality deals** (completeness < 50%):")
            if not low_q.empty:
                st.dataframe(low_q[["target_name", "data_origin", "completeness_score", "confidence_score"]]
                             .sort_values("completeness_score"), use_container_width=True)

    # --- Data Origin Audit ---
    with mgmt_tabs[3]:
        st.markdown("#### Data Origin Audit")
        audit = queries.get_data_origin_audit()
        if not audit.empty:
            fig = px.pie(audit, names="data_origin", values="deal_count",
                         title="Real vs. Synthetic Records",
                         template=TEMPLATE, color_discrete_sequence=[COLORS[0], COLORS[1]], hole=0.4)
            fig.update_layout(**DARK_BG)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(audit, use_container_width=True)

        missing_src = queries.get_missing_source_deals()
        if not missing_src.empty:
            st.markdown(f"**{len(missing_src)} real deals missing source URLs:**")
            st.dataframe(missing_src, use_container_width=True)
