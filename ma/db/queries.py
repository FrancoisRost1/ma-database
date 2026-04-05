"""
Reusable analytical queries — all SQL lives here, not in the app or analytics layers.
All functions return pandas DataFrames for downstream use.
No business logic here — only data retrieval and aggregation.
"""
import pandas as pd
from ma.db.engine import get_connection


def get_all_deals(filters: dict = None) -> pd.DataFrame:
    """
    Fetch all deals from v_deals_flat, optionally applying sidebar filter criteria.
    filters dict keys match the global sidebar filter spec in CLAUDE.md.
    """
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"SELECT * FROM v_deals_flat {where} ORDER BY announcement_date DESC"
    return conn.execute(sql, params).df()


def get_deal_by_id(deal_id: str) -> pd.DataFrame:
    """Fetch a single deal's full flat record by deal_id."""
    conn = get_connection()
    return conn.execute("SELECT * FROM v_deals_flat WHERE deal_id = ?", [deal_id]).df()


def get_deals_count(filters: dict = None) -> int:
    """Return total deal count matching the given filters."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"SELECT COUNT(*) FROM v_deals_flat {where}"
    return conn.execute(sql, params).fetchone()[0]


def get_kpi_summary(filters: dict = None) -> dict:
    """
    Compute dashboard KPI cards:
    - total deals / real / synthetic
    - total disclosed deal value
    - sponsor vs strategic split
    - median EV/EBITDA
    - most active sector
    - most active sponsor
    """
    conn = get_connection()
    where, params = _build_where(filters or {})
    df = conn.execute(f"SELECT * FROM v_deals_flat {where}", params).df()

    if df.empty:
        return {}

    sponsor_df = df[df["acquirer_type"] == "sponsor"]
    strategic_df = df[df["acquirer_type"] == "strategic"]

    most_active_sector = (
        df["sector_name"].value_counts().idxmax()
        if not df["sector_name"].dropna().empty else "N/A"
    )
    most_active_sponsor = (
        sponsor_df["acquirer_name"].value_counts().idxmax()
        if not sponsor_df["acquirer_name"].dropna().empty else "N/A"
    )

    return {
        "total_deals": len(df),
        "real_deals": int((df["data_origin"] == "real").sum()),
        "synthetic_deals": int((df["data_origin"] == "synthetic").sum()),
        "total_deal_value_usd": df["deal_value_usd"].sum(),
        "sponsor_deal_count": len(sponsor_df),
        "strategic_deal_count": len(strategic_df),
        "sponsor_deal_value": sponsor_df["deal_value_usd"].sum(),
        "strategic_deal_value": strategic_df["deal_value_usd"].sum(),
        "median_ev_to_ebitda": df["ev_to_ebitda"].median(),
        "most_active_sector": most_active_sector,
        "most_active_sponsor": most_active_sponsor,
    }


def get_deal_count_by_year(filters: dict = None) -> pd.DataFrame:
    """Annual deal count time series for the overview line chart."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT announcement_year AS year, COUNT(*) AS deal_count
        FROM v_deals_flat {where}
        GROUP BY announcement_year
        ORDER BY year
    """
    return conn.execute(sql, params).df()


def get_deal_value_by_year(filters: dict = None) -> pd.DataFrame:
    """Annual deal value (billions USD) time series."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT announcement_year AS year,
               SUM(deal_value_usd) AS total_value_usd
        FROM v_deals_flat {where}
        GROUP BY announcement_year
        ORDER BY year
    """
    return conn.execute(sql, params).df()


def get_deal_count_by_sector(filters: dict = None, top_n: int = 10) -> pd.DataFrame:
    """Top N sectors by deal count."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT sector_name, COUNT(*) AS deal_count
        FROM v_deals_flat {where}
        GROUP BY sector_name
        ORDER BY deal_count DESC
        LIMIT {int(top_n)}
    """
    return conn.execute(sql, params).df()


def get_deal_count_by_acquirer(filters: dict = None, top_n: int = 10) -> pd.DataFrame:
    """Top N acquirers by deal count."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "acquirer_name IS NOT NULL")
    sql = f"""
        SELECT acquirer_name, acquirer_type, COUNT(*) AS deal_count
        FROM v_deals_flat {w}
        GROUP BY acquirer_name, acquirer_type
        ORDER BY deal_count DESC
        LIMIT {int(top_n)}
    """
    return conn.execute(sql, params).df()


def get_deal_type_distribution(filters: dict = None) -> pd.DataFrame:
    """Deal type breakdown for donut chart."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT deal_type, COUNT(*) AS deal_count
        FROM v_deals_flat {where}
        GROUP BY deal_type
        ORDER BY deal_count DESC
    """
    return conn.execute(sql, params).df()


def get_ev_ebitda_by_sector(filters: dict = None) -> pd.DataFrame:
    """EV/EBITDA distribution data by sector for box plots."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "ev_to_ebitda IS NOT NULL AND sector_name IS NOT NULL")
    sql = f"SELECT sector_name, ev_to_ebitda FROM v_deals_flat {w} ORDER BY sector_name"
    return conn.execute(sql, params).df()


def get_ev_revenue_by_sector(filters: dict = None) -> pd.DataFrame:
    """EV/Revenue distribution data by sector for box plots."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "ev_to_revenue IS NOT NULL AND sector_name IS NOT NULL")
    sql = f"SELECT sector_name, ev_to_revenue FROM v_deals_flat {w} ORDER BY sector_name"
    return conn.execute(sql, params).df()


def get_premium_distribution(filters: dict = None) -> pd.DataFrame:
    """Premium paid % for public targets."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "target_status = 'public'")
    w = _and_condition(w, "premium_paid_pct IS NOT NULL")
    sql = f"SELECT sector_name, premium_paid_pct FROM v_deals_flat {w}"
    return conn.execute(sql, params).df()


def get_median_ev_ebitda_by_sector_year(filters: dict = None) -> pd.DataFrame:
    """Median EV/EBITDA by sector and year — valuation regime shift analysis."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "ev_to_ebitda IS NOT NULL AND sector_name IS NOT NULL")
    sql = f"""
        SELECT announcement_year AS year, sector_name,
               MEDIAN(ev_to_ebitda) AS median_ev_to_ebitda
        FROM v_deals_flat {w}
        GROUP BY announcement_year, sector_name
        ORDER BY year, sector_name
    """
    return conn.execute(sql, params).df()


def get_sponsor_vs_strategic_multiples(filters: dict = None) -> pd.DataFrame:
    """Average entry EV/EBITDA split by acquirer type (sponsor vs strategic)."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT acquirer_type,
               AVG(ev_to_ebitda)  AS avg_ev_to_ebitda,
               MEDIAN(ev_to_ebitda) AS median_ev_to_ebitda,
               COUNT(*) AS deal_count
        FROM v_deals_flat {_and_condition(where, 'ev_to_ebitda IS NOT NULL AND acquirer_type IS NOT NULL')}
        GROUP BY acquirer_type
    """
    return conn.execute(sql, params).df()


def get_sector_activity_heatmap(filters: dict = None) -> pd.DataFrame:
    """Deal count by sector × year — used for activity heatmap."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "sector_name IS NOT NULL")
    sql = f"""
        SELECT announcement_year AS year, sector_name, COUNT(*) AS deal_count
        FROM v_deals_flat {w}
        GROUP BY announcement_year, sector_name
        ORDER BY year, sector_name
    """
    return conn.execute(sql, params).df()


def get_sponsor_vs_strategic_trend(filters: dict = None) -> pd.DataFrame:
    """Annual sponsor vs strategic deal count — trend chart."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "acquirer_type IS NOT NULL")
    sql = f"""
        SELECT announcement_year AS year, acquirer_type, COUNT(*) AS deal_count
        FROM v_deals_flat {w}
        GROUP BY announcement_year, acquirer_type
        ORDER BY year, acquirer_type
    """
    return conn.execute(sql, params).df()


def get_deal_status_breakdown(filters: dict = None) -> pd.DataFrame:
    """Deal status distribution by year."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT announcement_year AS year, deal_status, COUNT(*) AS deal_count
        FROM v_deals_flat {where}
        GROUP BY announcement_year, deal_status
        ORDER BY year, deal_status
    """
    return conn.execute(sql, params).df()


def get_sector_value_treemap(filters: dict = None) -> pd.DataFrame:
    """Total deal value by sector for treemap."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "sector_name IS NOT NULL")
    sql = f"""
        SELECT sector_name, SUM(deal_value_usd) AS total_value_usd, COUNT(*) AS deal_count
        FROM v_deals_flat {w}
        GROUP BY sector_name
        ORDER BY total_value_usd DESC
    """
    return conn.execute(sql, params).df()


def get_sponsor_rankings(filters: dict = None, top_n: int = 15) -> pd.DataFrame:
    """Sponsor deal count, total value, avg deal size — for sponsor intelligence tab."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "acquirer_type = 'sponsor'")
    w = _and_condition(w, "acquirer_name IS NOT NULL")
    sql = f"""
        SELECT acquirer_name AS sponsor_name,
               COUNT(*)                    AS deal_count,
               SUM(deal_value_usd)         AS total_deal_value_usd,
               AVG(deal_value_usd)         AS avg_deal_size_usd,
               AVG(ev_to_ebitda)           AS avg_ev_to_ebitda
        FROM v_deals_flat {w}
        GROUP BY acquirer_name
        ORDER BY deal_count DESC, acquirer_name ASC
        LIMIT {int(top_n)}
    """
    return conn.execute(sql, params).df()


def get_sponsor_sector_heatmap(filters: dict = None, top_n_sponsors: int = 12) -> pd.DataFrame:
    """Sponsor × sector deal count matrix for heatmap."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "acquirer_type = 'sponsor'")
    w = _and_condition(w, "sector_name IS NOT NULL")
    w = _and_condition(w, "acquirer_name IS NOT NULL")

    # Get top sponsors by count first
    top_sponsors_sql = f"""
        SELECT acquirer_name FROM (
            SELECT acquirer_name, COUNT(*) AS n
            FROM v_deals_flat {w}
            GROUP BY acquirer_name
            ORDER BY n DESC
            LIMIT {int(top_n_sponsors)}
        )
    """
    top_sponsors = [r[0] for r in conn.execute(top_sponsors_sql, params).fetchall()]
    if not top_sponsors:
        return pd.DataFrame()

    placeholders = ", ".join(["?" for _ in top_sponsors])
    w2 = _and_condition(w, f"acquirer_name IN ({placeholders})")
    sql = f"""
        SELECT acquirer_name AS sponsor_name, sector_name, COUNT(*) AS deal_count
        FROM v_deals_flat {w2}
        GROUP BY acquirer_name, sector_name
        ORDER BY acquirer_name, sector_name
    """
    return conn.execute(sql, params + top_sponsors).df()


def get_sponsor_deal_trend(filters: dict = None, top_n_sponsors: int = 5) -> pd.DataFrame:
    """Annual deal count for top N sponsors — line chart."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "acquirer_type = 'sponsor'")
    w = _and_condition(w, "acquirer_name IS NOT NULL")

    top_sql = f"""
        SELECT acquirer_name FROM (
            SELECT acquirer_name, COUNT(*) AS n
            FROM v_deals_flat {w}
            GROUP BY acquirer_name ORDER BY n DESC LIMIT {int(top_n_sponsors)}
        )
    """
    top = [r[0] for r in conn.execute(top_sql, params).fetchall()]
    if not top:
        return pd.DataFrame()

    placeholders = ", ".join(["?" for _ in top])
    w2 = _and_condition(w, f"acquirer_name IN ({placeholders})")
    sql = f"""
        SELECT announcement_year AS year, acquirer_name AS sponsor_name, COUNT(*) AS deal_count
        FROM v_deals_flat {w2}
        GROUP BY announcement_year, acquirer_name
        ORDER BY year, acquirer_name
    """
    return conn.execute(sql, params + top).df()


def get_completeness_distribution(filters: dict = None) -> pd.DataFrame:
    """Completeness score distribution for data management tab."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    sql = f"""
        SELECT deal_id, target_name, data_origin,
               completeness_score, confidence_score
        FROM v_deals_flat {_and_condition(where, 'completeness_score IS NOT NULL')}
        ORDER BY completeness_score ASC
    """
    return conn.execute(sql, params).df()


def get_data_origin_audit() -> pd.DataFrame:
    """Count of real vs synthetic records."""
    conn = get_connection()
    return conn.execute("""
        SELECT data_origin, COUNT(*) AS deal_count
        FROM deals GROUP BY data_origin
    """).df()


def get_missing_source_deals(filters: dict = None) -> pd.DataFrame:
    """Deals with data_origin=real but no source_url."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "data_origin = 'real'")
    w = _and_condition(w, "source_url IS NULL")
    sql = f"""
        SELECT deal_id, target_name, announcement_date, acquirer_name, completeness_score
        FROM v_deals_flat {w}
        ORDER BY announcement_date DESC
    """
    return conn.execute(sql, params).df()


def get_low_quality_deals(min_score: float = 50.0, filters: dict = None) -> pd.DataFrame:
    """Deals below the completeness threshold."""
    conn = get_connection()
    where, params = _build_where(filters or {})
    w = _and_condition(where, "completeness_score < ?")
    sql = f"""
        SELECT deal_id, target_name, announcement_date, acquirer_name,
               data_origin, completeness_score
        FROM v_deals_flat {w}
        ORDER BY completeness_score ASC
    """
    return conn.execute(sql, params + [min_score]).df()


def party_exists(party_name: str) -> "tuple[bool, str]":
    """Check if a party already exists by name. Returns (found, party_id)."""
    conn = get_connection()
    row = conn.execute(
        "SELECT party_id FROM parties WHERE LOWER(party_name) = LOWER(?)", [party_name]
    ).fetchone()
    if row:
        return True, row[0]
    return False, ""


def sector_exists(sector_name: str, sub_industry: str = None) -> "tuple[bool, str]":
    """
    Check if a sector row already exists for the given sector_name + sub_industry.
    Returns (found, sector_id).
    """
    conn = get_connection()
    if sub_industry:
        row = conn.execute(
            "SELECT sector_id FROM sectors WHERE sector_name = ? AND sub_industry = ?",
            [sector_name, sub_industry],
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT sector_id FROM sectors WHERE sector_name = ? AND sub_industry IS NULL",
            [sector_name],
        ).fetchone()
    if row:
        return True, row[0]
    return False, ""


def insert_party(party: dict) -> None:
    """Insert a single party record."""
    conn = get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO parties VALUES (?, ?, ?, ?, ?)",
        [party["party_id"], party["party_name"], party["party_type"],
         party.get("headquarters"), party.get("description")],
    )


def insert_sector(sector: dict) -> None:
    """Insert a single sector record."""
    conn = get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO sectors VALUES (?, ?, ?)",
        [sector["sector_id"], sector["sector_name"], sector.get("sub_industry")],
    )


def insert_deal(deal: dict) -> None:
    """Insert a single deal record."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO deals VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        deal["deal_id"], deal["announcement_date"], deal.get("closing_date"),
        deal["deal_type"], deal["deal_status"], deal.get("deal_value_usd"),
        deal.get("deal_value_local"), deal.get("currency", "USD"),
        deal.get("enterprise_value"), deal.get("equity_value"),
        deal.get("acquirer_party_id"), deal["target_name"],
        deal.get("target_party_id"), deal.get("target_status"),
        deal.get("sector_id"), deal.get("geography", "US"),
        deal.get("minority_or_control"), deal.get("hostile_or_friendly"),
        deal.get("consortium_flag", False), deal.get("financing_structure_text"),
        deal.get("notes"), deal["data_origin"],
        deal.get("created_at"), deal.get("updated_at"),
    ])


def insert_valuation_metrics(vm: dict) -> None:
    """Insert valuation metrics for a deal."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO valuation_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        vm["deal_id"], vm.get("ev_to_ebitda"), vm.get("ev_to_revenue"),
        vm.get("premium_paid_pct"), vm.get("leverage_multiple"),
        vm.get("target_revenue"), vm.get("target_ebitda"),
        vm.get("target_ebitda_margin"),
    ])


def insert_deal_metadata(dm: dict) -> None:
    """Insert deal metadata (quality scores, source)."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO deal_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        dm["deal_id"], dm.get("data_source"), dm.get("source_url"),
        dm.get("citation"), dm.get("completeness_score"),
        dm.get("confidence_score"), dm.get("last_reviewed"),
        dm.get("reviewed_by"),
    ])


def update_deal_scores(deal_id: str, completeness_score: float, confidence_score: float) -> None:
    """Update quality scores for an existing deal_metadata record."""
    conn = get_connection()
    conn.execute("""
        UPDATE deal_metadata
        SET completeness_score = ?, confidence_score = ?
        WHERE deal_id = ?
    """, [completeness_score, confidence_score, deal_id])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _and_condition(where: str, condition: str) -> str:
    """Safely append a condition to WHERE clause or start a new one."""
    if where:
        return where + f" AND {condition}"
    return f"WHERE {condition}"


# ---------------------------------------------------------------------------
# Internal filter builder
# ---------------------------------------------------------------------------

def _build_where(filters: dict) -> "tuple[str, list]":
    """
    Build a WHERE clause and params list from the global sidebar filter dict.
    Returns ("WHERE ...", [params]) or ("", []) if no filters.
    """
    clauses = []
    params = []

    if "year_start" in filters:
        clauses.append("announcement_year >= ?")
        params.append(filters["year_start"])
    if "year_end" in filters:
        clauses.append("announcement_year <= ?")
        params.append(filters["year_end"])
    if "sectors" in filters and filters["sectors"]:
        placeholders = ", ".join(["?" for _ in filters["sectors"]])
        clauses.append(f"sector_name IN ({placeholders})")
        params.extend(filters["sectors"])
    if "sub_industries" in filters and filters["sub_industries"]:
        placeholders = ", ".join(["?" for _ in filters["sub_industries"]])
        clauses.append(f"sub_industry IN ({placeholders})")
        params.extend(filters["sub_industries"])
    if "deal_types" in filters and filters["deal_types"]:
        placeholders = ", ".join(["?" for _ in filters["deal_types"]])
        clauses.append(f"deal_type IN ({placeholders})")
        params.extend(filters["deal_types"])
    if "acquirer_types" in filters and filters["acquirer_types"]:
        placeholders = ", ".join(["?" for _ in filters["acquirer_types"]])
        clauses.append(f"acquirer_type IN ({placeholders})")
        params.extend(filters["acquirer_types"])
    if "acquirer_names" in filters and filters["acquirer_names"]:
        placeholders = ", ".join(["?" for _ in filters["acquirer_names"]])
        clauses.append(f"acquirer_name IN ({placeholders})")
        params.extend(filters["acquirer_names"])
    if "deal_statuses" in filters and filters["deal_statuses"]:
        placeholders = ", ".join(["?" for _ in filters["deal_statuses"]])
        clauses.append(f"deal_status IN ({placeholders})")
        params.extend(filters["deal_statuses"])
    if "geographies" in filters and filters["geographies"]:
        placeholders = ", ".join(["?" for _ in filters["geographies"]])
        clauses.append(f"geography IN ({placeholders})")
        params.extend(filters["geographies"])
    if "deal_value_min" in filters:
        clauses.append("deal_value_usd >= ?")
        params.append(filters["deal_value_min"])
    if "deal_value_max" in filters and filters["deal_value_max"] < 999999:
        clauses.append("deal_value_usd <= ?")
        params.append(filters["deal_value_max"])
    if "data_origin" in filters and filters["data_origin"] != "all":
        clauses.append("data_origin = ?")
        params.append(filters["data_origin"])
    if "completeness_min" in filters and filters["completeness_min"] > 0:
        clauses.append("completeness_score >= ?")
        params.append(filters["completeness_min"])

    if clauses:
        return "WHERE " + " AND ".join(clauses), params
    return "", params
