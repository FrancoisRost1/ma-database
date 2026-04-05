"""
Schema manager — table creation, constraints, and analytical views.
All DDL lives here. No DDL in other modules.
Five tables + two views: deals, parties, sectors, valuation_metrics, deal_metadata.
"""
import duckdb
from ma.db.engine import get_connection


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

_CREATE_PARTIES = """
CREATE TABLE IF NOT EXISTS parties (
    party_id        VARCHAR PRIMARY KEY,
    party_name      VARCHAR NOT NULL UNIQUE,
    party_type      VARCHAR NOT NULL,   -- sponsor / strategic / consortium / other
    headquarters    VARCHAR,
    description     VARCHAR
);
"""

_CREATE_SECTORS = """
CREATE TABLE IF NOT EXISTS sectors (
    sector_id       VARCHAR PRIMARY KEY,
    sector_name     VARCHAR NOT NULL,
    sub_industry    VARCHAR,
    UNIQUE (sector_name, sub_industry)
);
"""

_CREATE_DEALS = """
CREATE TABLE IF NOT EXISTS deals (
    deal_id                   VARCHAR PRIMARY KEY,
    announcement_date         DATE    NOT NULL,
    closing_date              DATE,
    deal_type                 VARCHAR NOT NULL,   -- lbo/strategic_acquisition/merger/take_private/carve_out
    deal_status               VARCHAR NOT NULL,   -- announced/closed/terminated/pending
    deal_value_usd            DOUBLE,             -- millions USD
    deal_value_local          DOUBLE,
    currency                  VARCHAR DEFAULT 'USD',
    enterprise_value          DOUBLE,             -- millions USD
    equity_value              DOUBLE,             -- millions USD
    acquirer_party_id         VARCHAR REFERENCES parties(party_id),
    target_name               VARCHAR NOT NULL,
    target_party_id           VARCHAR REFERENCES parties(party_id),
    target_status             VARCHAR,            -- public/private/subsidiary/carve_out
    sector_id                 VARCHAR REFERENCES sectors(sector_id),
    geography                 VARCHAR DEFAULT 'US',
    minority_or_control       VARCHAR,            -- control/minority/unknown
    hostile_or_friendly       VARCHAR,            -- hostile/friendly/unknown
    consortium_flag           BOOLEAN DEFAULT false,
    financing_structure_text  VARCHAR,
    notes                     VARCHAR,
    data_origin               VARCHAR NOT NULL,   -- real/synthetic — NEVER null
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_VALUATION_METRICS = """
CREATE TABLE IF NOT EXISTS valuation_metrics (
    deal_id               VARCHAR PRIMARY KEY REFERENCES deals(deal_id),
    ev_to_ebitda          DOUBLE,
    ev_to_revenue         DOUBLE,
    premium_paid_pct      DOUBLE,   -- e.g. 25.0 = 25%
    leverage_multiple     DOUBLE,   -- total debt / EBITDA at entry
    target_revenue        DOUBLE,   -- millions USD, LTM
    target_ebitda         DOUBLE,   -- millions USD, LTM
    target_ebitda_margin  DOUBLE    -- percentage
);
"""

_CREATE_DEAL_METADATA = """
CREATE TABLE IF NOT EXISTS deal_metadata (
    deal_id             VARCHAR PRIMARY KEY REFERENCES deals(deal_id),
    data_source         VARCHAR,
    source_url          VARCHAR,
    citation            VARCHAR,
    completeness_score  DOUBLE,
    confidence_score    DOUBLE,
    last_reviewed       DATE,
    reviewed_by         VARCHAR
);
"""

# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

_CREATE_VIEW_FLAT = """
CREATE OR REPLACE VIEW v_deals_flat AS
SELECT
    d.deal_id,
    d.announcement_date,
    YEAR(d.announcement_date)        AS announcement_year,
    QUARTER(d.announcement_date)     AS announcement_quarter,
    d.closing_date,
    d.deal_type,
    d.deal_status,
    d.deal_value_usd,
    d.enterprise_value,
    d.equity_value,
    d.target_name,
    d.target_status,
    d.geography,
    d.minority_or_control,
    d.hostile_or_friendly,
    d.consortium_flag,
    d.financing_structure_text,
    d.notes,
    d.data_origin,
    d.created_at,
    d.updated_at,
    -- party fields
    p.party_name      AS acquirer_name,
    p.party_type      AS acquirer_type,
    p.headquarters    AS acquirer_hq,
    -- sector fields
    s.sector_name,
    s.sub_industry,
    -- valuation metrics
    vm.ev_to_ebitda,
    vm.ev_to_revenue,
    vm.premium_paid_pct,
    vm.leverage_multiple,
    vm.target_revenue,
    vm.target_ebitda,
    vm.target_ebitda_margin,
    -- quality scores
    dm.completeness_score,
    dm.confidence_score,
    dm.data_source,
    dm.source_url,
    dm.citation
FROM deals d
LEFT JOIN parties         p  ON d.acquirer_party_id = p.party_id
LEFT JOIN sectors         s  ON d.sector_id         = s.sector_id
LEFT JOIN valuation_metrics vm ON d.deal_id         = vm.deal_id
LEFT JOIN deal_metadata   dm ON d.deal_id           = dm.deal_id;
"""

_CREATE_VIEW_SUMMARY = """
CREATE OR REPLACE VIEW v_deals_summary AS
SELECT
    YEAR(d.announcement_date)  AS year,
    s.sector_name,
    d.deal_type,
    p.party_type               AS acquirer_type,
    d.data_origin,
    COUNT(*)                   AS deal_count,
    SUM(d.deal_value_usd)      AS total_deal_value_usd,
    AVG(vm.ev_to_ebitda)       AS avg_ev_to_ebitda,
    MEDIAN(vm.ev_to_ebitda)    AS median_ev_to_ebitda,
    AVG(d.deal_value_usd)      AS avg_deal_value_usd
FROM deals d
LEFT JOIN parties           p  ON d.acquirer_party_id = p.party_id
LEFT JOIN sectors           s  ON d.sector_id         = s.sector_id
LEFT JOIN valuation_metrics vm ON d.deal_id           = vm.deal_id
GROUP BY
    YEAR(d.announcement_date),
    s.sector_name,
    d.deal_type,
    p.party_type,
    d.data_origin;
"""


def create_schema() -> None:
    """Create all tables and views. Safe to call multiple times (IF NOT EXISTS)."""
    conn = get_connection()
    conn.execute(_CREATE_PARTIES)
    conn.execute(_CREATE_SECTORS)
    conn.execute(_CREATE_DEALS)
    conn.execute(_CREATE_VALUATION_METRICS)
    conn.execute(_CREATE_DEAL_METADATA)
    conn.execute(_CREATE_VIEW_FLAT)
    conn.execute(_CREATE_VIEW_SUMMARY)


def drop_all_tables(confirm: bool = False) -> None:
    """
    Drop all tables and views — destructive, for test teardown only.
    Requires explicit confirm=True to prevent accidental calls.
    """
    if not confirm:
        raise ValueError("Pass confirm=True to drop all tables.")
    conn = get_connection()
    conn.execute("DROP VIEW IF EXISTS v_deals_summary")
    conn.execute("DROP VIEW IF EXISTS v_deals_flat")
    conn.execute("DROP TABLE IF EXISTS deal_metadata")
    conn.execute("DROP TABLE IF EXISTS valuation_metrics")
    conn.execute("DROP TABLE IF EXISTS deals")
    conn.execute("DROP TABLE IF EXISTS parties")
    conn.execute("DROP TABLE IF EXISTS sectors")


def table_exists(table_name: str) -> bool:
    """Return True if the named table exists in the database."""
    conn = get_connection()
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0
