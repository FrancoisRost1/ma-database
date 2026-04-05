# CLAUDE.md — M&A Database + Analysis Tool

> Project 4 in the God-Tier Finance GitHub series.
> Read this fully before writing any code.
> This is the single source of truth for this project.

---

## Project identity

**Name:** M&A Database + Analysis Tool
**Repo:** `ma-database`
**Status:** ✅ COMPLETE
**Master CLAUDE.md:** `../CLAUDE.md`

### What it is

An institutional-style M&A intelligence platform.
Stores historical deal data in a structured relational database, surfaces analytics on valuation regimes, sector activity, sponsor behavior, market imbalance signals, and relative valuation — with analyst-style interpretation.

### What it is NOT

- Not a web scraper
- Not a CSV viewer with filters
- Not a toy Streamlit app with random charts

### What it signals

1. Structured financial data modeling (relational schema, data quality scoring)
2. Transaction analysis thinking (sponsor vs strategic, valuation regime shifts, market imbalance detection)
3. Institutional analytics with interpretation edge (regime classification, relative valuation, sponsor behavioral profiling)
4. Clean architecture for future expansion (modular ingestion, pluggable data sources)

---

## Current state

- **Total deals:** 390 (90 real, 300 synthetic)
- **Sectors:** 11 (GICS-style)
- **Sponsors:** 24 represented
- **Tests:** 124/124 passing (62 original + 62 hardened)
- **Codex audit:** completed, all findings resolved
- **Dashboard:** 6 tabs, fully functional

---

## Repo structure

```
ma-database/
├── main.py                          # Orchestrator only — runs pipeline
├── config.yaml                      # All parameters, weights, thresholds
├── CLAUDE.md                        # This file
├── README.md                        # Project description + screenshots
├── docs/
│   ├── analysis.md                  # Investment-style write-up on M&A trends
│   └── schema_diagram.png           # Relational schema diagram for README
│
├── ma/
│   ├── __init__.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── engine.py                # DuckDB connection manager, abstraction layer
│   │   ├── schema.py                # Table creation, migrations, views, uniqueness constraints
│   │   └── queries.py               # Reusable analytical queries (no raw SQL in app layer)
│   │
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── seed_real.py             # Load curated real deal dataset + consortium seeding
│   │   ├── seed_synthetic.py        # Generate synthetic extension layer (recency-weighted)
│   │   ├── csv_import.py            # Bulk CSV import with validation
│   │   └── validator.py             # Field validation, duplicate detection, future-date guard
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── deal.py                  # Deal dataclass / schema definition
│   │   ├── party.py                 # Party (sponsor / strategic acquirer) model
│   │   └── sector.py                # Sector taxonomy model
│   │
│   ├── analytics/
│   │   ├── __init__.py
│   │   ├── valuation.py             # EV/EBITDA by sector, EV/Revenue, premium analysis
│   │   ├── market_activity.py       # Deal count/value trends, sector heatmaps
│   │   ├── sponsor_intel.py         # Sponsor rankings, preferences, entry multiples
│   │   ├── execution.py             # Announced vs closed vs terminated, time-to-close
│   │   ├── snapshot.py              # Strategic M&A Market Snapshot memo (5-section)
│   │   ├── regime.py                # Market regime detection and classification
│   │   ├── sponsor_profile.py       # Per-sponsor behavioral profiling
│   │   ├── relative_valuation.py    # Sector premium/discount vs market, historical percentile
│   │   ├── imbalance.py             # Market imbalance / momentum signal detection
│   │   └── interpretation.py        # Rule-based analyst commentary engine
│   │
│   ├── scoring/
│   │   ├── __init__.py
│   │   ├── completeness.py          # Weighted completeness score per deal
│   │   └── confidence.py            # Rule-based confidence score per deal
│   │
│   ├── export/
│   │   ├── __init__.py
│   │   ├── csv_export.py            # Filtered CSV export
│   │   └── excel_export.py          # Formatted Excel export with key tables
│   │
│   └── utils/
│       ├── __init__.py
│       ├── config_loader.py         # Load config.yaml, pass as dict
│       └── formatting.py            # Currency formatting, date parsing, display helpers
│
├── data/
│   ├── raw/
│   │   └── real_deals.csv           # Curated real deal dataset (90 deals)
│   ├── processed/
│   │   └── deals_combined.csv       # Real + synthetic, post-validation
│   └── ma_database.duckdb           # Primary DuckDB database file
│
├── app/
│   └── streamlit_app.py             # Dashboard — 6 tabs, dark mode, global filters
│
├── tests/
│   ├── __init__.py
│   ├── test_schema.py
│   ├── test_ingest.py
│   ├── test_validator.py
│   ├── test_completeness.py
│   ├── test_confidence.py
│   ├── test_valuation.py
│   ├── test_market_activity.py
│   ├── test_sponsor_intel.py
│   ├── test_snapshot.py
│   ├── test_export.py
│   ├── test_integration.py
│   └── test_hardened.py             # 62 stress tests (math, edge cases, integrity)
│
└── outputs/
    └── (exported files land here)
```

---

## Relational schema

### Design philosophy

Normalized enough to look institutional, simple enough to remain usable.
Five core tables + two analytical views.

### Table: `deals`

Primary transaction record.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| deal_id | VARCHAR | PK | UUID or sequential ID |
| announcement_date | DATE | YES | |
| closing_date | DATE | no | null if not closed or unknown |
| deal_type | VARCHAR | YES | LBO / strategic_acquisition / merger / take_private / carve_out |
| deal_status | VARCHAR | YES | announced / closed / terminated / pending |
| deal_value_usd | DOUBLE | no | in millions USD |
| deal_value_local | DOUBLE | no | original currency amount |
| currency | VARCHAR | no | ISO code, default USD |
| enterprise_value | DOUBLE | no | in millions USD |
| equity_value | DOUBLE | no | in millions USD |
| acquirer_party_id | VARCHAR | FK→parties | |
| target_name | VARCHAR | YES | |
| target_party_id | VARCHAR | FK→parties | nullable |
| target_status | VARCHAR | no | public / private / subsidiary / carve_out |
| sector_id | VARCHAR | FK→sectors | |
| geography | VARCHAR | YES | default "US" |
| minority_or_control | VARCHAR | no | control / minority / unknown |
| hostile_or_friendly | VARCHAR | no | hostile / friendly / unknown |
| consortium_flag | BOOLEAN | no | default false |
| financing_structure_text | VARCHAR | no | free text |
| notes | VARCHAR | no | |
| data_origin | VARCHAR | YES | real / synthetic — NEVER nullable |
| created_at | TIMESTAMP | auto | |
| updated_at | TIMESTAMP | auto | |

### Table: `parties`

Unified entity table for sponsors and strategic acquirers.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| party_id | VARCHAR | PK | |
| party_name | VARCHAR | YES, UNIQUE | e.g., "Blackstone", "Microsoft" |
| party_type | VARCHAR | YES | sponsor / strategic / consortium / other |
| headquarters | VARCHAR | no | |
| description | VARCHAR | no | |

### Table: `sectors`

GICS-style sector taxonomy.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| sector_id | VARCHAR | PK | |
| sector_name | VARCHAR | YES | e.g., "Technology", "Healthcare" |
| sub_industry | VARCHAR | no | UNIQUE(sector_name, sub_industry) |

### Table: `valuation_metrics`

One-to-one with deals.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| deal_id | VARCHAR | PK, FK→deals | |
| ev_to_ebitda | DOUBLE | no | |
| ev_to_revenue | DOUBLE | no | |
| premium_paid_pct | DOUBLE | no | percentage, e.g., 25.0 = 25% |
| leverage_multiple | DOUBLE | no | total debt / EBITDA at entry |
| target_revenue | DOUBLE | no | in millions USD, LTM |
| target_ebitda | DOUBLE | no | in millions USD, LTM |
| target_ebitda_margin | DOUBLE | no | percentage |

### Table: `deal_metadata`

Source tracking and quality scores.

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| deal_id | VARCHAR | PK, FK→deals | |
| data_source | VARCHAR | no | |
| source_url | VARCHAR | no | |
| citation | VARCHAR | no | |
| completeness_score | DOUBLE | computed | weighted % of key fields filled |
| confidence_score | DOUBLE | computed | rule-based trust rating |
| last_reviewed | DATE | no | |
| reviewed_by | VARCHAR | no | |

### View: `v_deals_flat`

Denormalized join of all 5 tables. Used by dashboard and exports. No business logic — just joins.

### View: `v_deals_summary`

Pre-aggregated summary by year, sector, deal_type, acquirer_type.

---

## Key formulas and logic

### Completeness scoring

```
completeness_score = sum(weight_i for each non-null field_i) / sum(all weights) × 100
```

| Tier | Weight | Fields |
|------|--------|--------|
| Tier 1 | 3.0 | announcement_date, acquirer_party_id, target_name, deal_type, sector_id, deal_value_usd, deal_status |
| Tier 2 | 2.0 | ev_to_ebitda, ev_to_revenue, premium_paid_pct, acquirer_type, closing_date, geography, enterprise_value, target_ebitda, target_revenue |
| Tier 3 | 1.0 | financing_structure_text, leverage_multiple, hostile_or_friendly, notes, source_url, sub_industry |

Total weight = (7×3) + (9×2) + (6×1) = 21 + 18 + 6 = **45**

Thresholds: ≥80% = High (green), 50-79% = Medium (yellow), <50% = Low (red)

### Confidence scoring

| Condition | Score |
|-----------|-------|
| data_origin = "real" AND source_url not null | 1.0 |
| data_origin = "real" AND source_url null | 0.8 |
| data_origin = "synthetic" | 0.5 |
| completeness < 30% | capped at 0.3 |

### Regime classification

For each year, compute deal count and median EV/EBITDA relative to full-period medians:

```
activity_level  = deal_count vs median(all years)    → High / Low
valuation_level = median_ev_ebitda vs median(all years) → High / Low
sponsor_pct     = sponsor deals / total deals        → Sponsor-Led (>45%) / Strategic-Led (<35%) / Balanced
```

| Activity | Valuation | Regime Label |
|----------|-----------|--------------|
| High | High | Peak / Late-Cycle |
| High | Low | Recovery / Opportunity |
| Low | High | Selective / Cautious |
| Low | Low | Trough / Distressed |

### Relative valuation

```
sector_premium     = sector_median_ev_ebitda - market_median_ev_ebitda
historical_pctile  = percentile_rank(current_sector_median, sector_10yr_history)
sponsor_spread     = sponsor_median_ev_ebitda - strategic_median_ev_ebitda (per sector)
```

### Market imbalance detection

```
activity_momentum  = (deal_count_last_2yr / deal_count_prior_3yr - 1) × 100
valuation_momentum = (median_ev_ebitda_last_2yr / median_ev_ebitda_prior_3yr - 1) × 100
```

| Activity Momentum | Valuation Momentum | Signal |
|--------------------|--------------------|--------|
| Accelerating | Rising | Overheating |
| Accelerating | Flat/Falling | Healthy Growth |
| Declining | Rising | Narrowing |
| Declining | Falling | Cooling |

### Sponsor profiling

```
valuation_stance:
  avg_ev_ebitda > market_median + 1.0x  → "Premium Buyer"
  avg_ev_ebitda < market_median - 1.0x  → "Value Buyer"
  else                                   → "Market Buyer"
```

Per sponsor: preferred sectors, avg deal size vs market, regime activity timing, deal type preference.

---

## Analytics specification

### Tab 1: OVERVIEW

**KPI cards (2 rows of 4):**
- Total deals, total disclosed deal value, sponsor/strategic split %, median EV/EBITDA
- Sponsor count, strategic count, most active sector, most active sponsor

**Data composition block:**
- Real deals count + %, synthetic deals count + %, combined universe total

**Charts:**
- Deal count over time (line chart, annual)
- Top 10 sectors by deal count (horizontal bar)
- Top 10 sponsors by deal count (horizontal bar)
- Deal type distribution (donut/pie)
- Regime timeline (color-coded bar by year)
- Current regime callout card

**M&A Market Snapshot memo** — 5-section strategic commentary (see below)

---

### Tab 2: VALUATION

**Charts:**
- EV/EBITDA distribution by sector (box plot — primary)
- EV/Revenue distribution by sector (box plot)
- Premium paid distribution for public targets (histogram + box plot)
- Median EV/EBITDA over time by sector (line chart — regime shifts)
- Sponsor vs strategic entry multiples (grouped bar)

**Relative Valuation section:**
- Sector premium/discount vs market (horizontal bar, sorted)
- Historical percentile positioning per sector (horizontal bar)
- Sponsor vs strategic spread by sector (grouped bar with spread labels)
- Relative valuation narrative text block

---

### Tab 3: MARKET ACTIVITY

**Charts:**
- Deal count over time (line chart, annual/quarterly toggle)
- Deal value over time (line/area chart)
- Sector activity heatmap (sector × year)
- Sponsor vs strategic activity trends (stacked/grouped bar)
- Deal status breakdown (stacked bar)
- Sector deal value treemap (secondary)

**Market Signals section:**
- Sector quadrant scatter (activity momentum × valuation momentum, colored by signal)
- Signal table with all sectors + interpretation
- Imbalance narrative text block

---

### Tab 4: SPONSOR INTELLIGENCE

**Charts:**
- Most active sponsors by deal count (horizontal bar)
- Most active sponsors by deal value (horizontal bar)
- Average deal size by sponsor (bar)
- Sponsor sector preferences (heatmap)
- Sponsor average entry EV/EBITDA (bar)
- Sponsor deal frequency over time (line, top sponsors)

**Sponsor Profiles section:**
- Summary table: all sponsors with valuation stance, preferred sectors, deal count
- Dropdown → individual sponsor profile card with full narrative

---

### Tab 5: DEAL EXPLORER

- Full filterable/searchable deal table
- Column sorting
- Deal detail drill-down
- Synthetic deals clearly labeled
- CSV + Excel export buttons

---

### Tab 6: DATA MANAGEMENT

**System Architecture card:**
- Pipeline flow: Ingest → Validate → Score → Store → Analyze → Export
- Component grid showing all pipeline modules

**Scoring Methodology explanation:**
- Completeness tier weights and quality thresholds
- Confidence scoring rules
- How to filter low-quality records

**Data management features:**
- Add new deal form
- Edit existing deal
- Bulk CSV import with validation preview + duplicate detection
- Completeness distribution chart
- Low-quality deal list
- Data origin audit
- Source/citation audit

---

### M&A Market Snapshot (strategic memo)

5-section analyst-style commentary, respects current filters.

1. **Market Regime** — current classification + recent transition narrative
2. **Sector Signals** — top 2-3 sectors with imbalance signals + relative valuation context
3. **Sponsor Activity** — top sponsors with behavioral profiling context
4. **Valuation Environment** — market-wide valuation vs history, sponsor vs strategic spread
5. **Watch List** — 2-3 actionable items to monitor

Max 400 words. Every observation ends with "which suggests..." or "investors should watch..."

---

### Sidebar: Platform Methodology

Collapsible expander showing:
- Data architecture (5 tables, 2 views, DuckDB)
- Data sources (real curated + synthetic extension)
- Quality scoring methodology
- Analytics engine capabilities

---

## Global sidebar filters

| Filter | Type | Default |
|--------|------|---------|
| Date range | Start/end year slider | Full range |
| Sector | Multi-select | All |
| Sub-industry | Multi-select (sector-aware) | All |
| Deal type | Multi-select | All |
| Acquirer type | Multi-select | All |
| Acquirer / sponsor name | Searchable multi-select | All |
| Deal status | Multi-select | All |
| Geography | Multi-select | All |
| Deal value range | Min/max slider | Full range |
| Data origin | Radio | Real (default) |
| Completeness threshold | Slider 0-100% | 0% |

---

## Seed data strategy

### Real core (90 deals)

Curated from publicly known transactions. Covers major PE sponsors (Blackstone, KKR, Apollo, Carlyle, Bain, Vista, Thoma Bravo, Silver Lake, Warburg Pincus, TPG, Advent, Hellman & Friedman, EQT, Permira, Brookfield, IFM Investors) and strategic acquirers (Microsoft, Broadcom, Oracle, Salesforce, etc.). Spread 2016-2026. `data_origin = "real"`.

### Synthetic extension (300 deals)

Generated with realistic distributions. Recency-weighted (2026 = 24.4%, 2016 = 1.8%). LBO deals are ~90% sponsor-led. `data_origin = "synthetic"`. Clearly labeled and filterable everywhere.

### Consortium vehicles

5 consortium parties seeded (Blackstone/KKR, KKR/Carlyle, Apollo/TPG, Blackstone/Carlyle, KKR/Warburg Pincus).

---

## Key design decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database | DuckDB | Columnar, fast aggregation, single file |
| Schema | Relational + flat views | Institutional, extensible |
| Seed data | Real core + synthetic extension | Credibility + analytical depth |
| Parties | Unified parties table | One framework for all counterparties |
| Currency | USD normalized | Phase 1 US-focused |
| Time granularity | Annual default, quarterly optional | Stable trends at this dataset size |
| Manual entry | Streamlit form + CSV import | Complete and practical |
| Default data view | Real only | First impression = credible data |
| Regime detection | 4-quadrant activity × valuation | Interpretable market environment labels |
| Sponsor profiling | Premium/Value/Market buyer | Behavioral classification with narrative |
| Relative valuation | Premium/discount + percentile | Goes beyond absolute multiples |
| Imbalance detection | Momentum-based signals | Identifies overheating / cooling sectors |
| Snapshot memo | 5-section strategic format | Analyst commentary, not data dump |
| Interpretation layer | Rule-based financial language | "Control premium", "leverage capacity", "dry powder" |

---

## Simplifying assumptions (documented in code)

- Deal values as reported; no inflation adjustment
- EV/EBITDA and EV/Revenue as reported; no independent recalculation
- Premium based on reported figures; no pre-announcement price lookback
- Synthetic deals use realistic distributions but are not based on real transactions
- Sector classification is GICS-style simplified (11 sectors + sub-industries)
- Time-to-close = closing_date − announcement_date; no regulatory delay adjustment
- Currency conversion not implemented; all values assumed USD
- Consortium deals attributed to primary sponsor for ranking
- Single acquirer per deal (no multi-acquirer JVs)
- Regime classification uses simple median thresholds, not rolling windows
- Imbalance detection uses 2yr vs 3yr split; no statistical significance testing
- Sponsor valuation stance uses ±1.0x threshold from market median

---

## Data origin rules (CRITICAL)

1. Every record MUST have `data_origin` = `real` or `synthetic` — NEVER null
2. Synthetic records filterable in all views
3. Dashboard supports real-only / synthetic-only / combined
4. Exports preserve `data_origin` column
5. Deal detail views clearly label synthetic records
6. Synthetic data NEVER creates impression of a real transaction
7. Dashboard defaults to "Real" view

---

## Dependencies

```
pandas
numpy
duckdb
pyyaml
streamlit
plotly
openpyxl
pytest
uuid (stdlib)
```

---

## Future extensions (not Phase 1)

- Light scraping / ingestion from public deal lists and press releases
- SEC EDGAR filing ingestion
- Exit data tracking (holding period, exit multiple, MOIC)
- Advisor name tracking
- PDF quarterly M&A market report generation
- Global deal expansion with FX conversion
- API connectors for deal data providers
- Event study: acquirer stock price reaction
- Rolling-window regime detection
- Statistical significance on imbalance signals
- Multi-factor sponsor scoring model

---

*Project CLAUDE.md — M&A Database + Analysis Tool*
*Last updated: 2026-04-05*
*Status: ✅ COMPLETE — 390 deals, 124/124 tests, Codex audited, 6 analytics upgrades applied*