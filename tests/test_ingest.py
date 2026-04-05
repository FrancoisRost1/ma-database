"""Tests: seed loading, synthetic generation, and basic counts."""
from ma.db import queries


def test_real_seed_inserts_deals(db, config):
    from ma.ingest.seed_real import seed_real_deals
    inserted = seed_real_deals(config)
    assert inserted > 0, "Should insert at least one real deal"
    assert queries.get_deals_count() == inserted


def test_real_seed_data_origin(db, config):
    from ma.ingest.seed_real import seed_real_deals
    seed_real_deals(config)
    from ma.db.engine import get_connection
    conn = get_connection()
    result = conn.execute("SELECT COUNT(*) FROM deals WHERE data_origin != 'real'").fetchone()
    assert result[0] == 0, "All real seeds must have data_origin='real'"


def test_synthetic_seed_inserts_deals(db, config):
    from ma.ingest.seed_real import seed_real_deals
    from ma.ingest.seed_synthetic import seed_synthetic_deals
    seed_real_deals(config)
    existing = queries.get_deals_count()
    inserted = seed_synthetic_deals(config, existing)
    assert inserted > 0


def test_synthetic_data_origin(db, config):
    from ma.ingest.seed_real import seed_real_deals
    from ma.ingest.seed_synthetic import seed_synthetic_deals
    seed_real_deals(config)
    existing = queries.get_deals_count()
    seed_synthetic_deals(config, existing)
    from ma.db.engine import get_connection
    conn = get_connection()
    result = conn.execute("SELECT COUNT(*) FROM deals WHERE data_origin = 'synthetic'").fetchone()
    assert result[0] > 0


def test_no_null_data_origin(seeded_db):
    from ma.db.engine import get_connection
    conn = get_connection()
    result = conn.execute("SELECT COUNT(*) FROM deals WHERE data_origin IS NULL").fetchone()
    assert result[0] == 0, "data_origin must never be null"


def test_valuation_metrics_linked(seeded_db):
    from ma.db.engine import get_connection
    conn = get_connection()
    # Every deal should have a valuation_metrics row
    result = conn.execute("""
        SELECT COUNT(*) FROM deals d
        LEFT JOIN valuation_metrics vm ON d.deal_id = vm.deal_id
        WHERE vm.deal_id IS NULL
    """).fetchone()
    assert result[0] == 0, "Every deal must have a valuation_metrics row"


def test_deal_metadata_linked(seeded_db):
    from ma.db.engine import get_connection
    conn = get_connection()
    result = conn.execute("""
        SELECT COUNT(*) FROM deals d
        LEFT JOIN deal_metadata dm ON d.deal_id = dm.deal_id
        WHERE dm.deal_id IS NULL
    """).fetchone()
    assert result[0] == 0, "Every deal must have a deal_metadata row"
