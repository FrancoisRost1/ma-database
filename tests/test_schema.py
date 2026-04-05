"""Tests: table creation, constraints, and views."""
from ma.db.schema import table_exists


def test_all_tables_created(db):
    for table in ["deals", "parties", "sectors", "valuation_metrics", "deal_metadata"]:
        assert table_exists(table), f"Table {table} not created"


def test_views_accessible(db):
    from ma.db.engine import get_connection
    conn = get_connection()
    # Views return without error even when empty
    result = conn.execute("SELECT COUNT(*) FROM v_deals_flat").fetchone()
    assert result[0] == 0

    result = conn.execute("SELECT COUNT(*) FROM v_deals_summary").fetchone()
    assert result[0] == 0


def test_drop_and_recreate(db):
    from ma.db.schema import drop_all_tables, create_schema
    drop_all_tables(confirm=True)
    assert not table_exists("deals")
    create_schema()
    assert table_exists("deals")
