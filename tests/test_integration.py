"""
Integration test: end-to-end pipeline — seed → score → query → export.
Validates the full flow without mocking any component.
"""
import os
from ma.db import queries


def test_full_pipeline_real_seed(db, config, tmp_path):
    from ma.ingest.seed_real import seed_real_deals
    from ma.analytics import valuation, market_activity, sponsor_intel
    from ma.analytics.snapshot import generate_snapshot
    from ma.export.csv_export import export_deals_csv

    # Step 1: Seed
    inserted = seed_real_deals(config)
    assert inserted > 0

    # Step 2: Query
    kpis = queries.get_kpi_summary()
    assert kpis["total_deals"] == inserted
    assert kpis["real_deals"] == inserted
    assert kpis["synthetic_deals"] == 0

    # Step 3: Analytics
    ev_df = valuation.ev_ebitda_by_sector()
    assert not ev_df.empty

    cnt_df = market_activity.deal_count_over_time()
    assert not cnt_df.empty

    sponsor_df = sponsor_intel.sponsor_rankings()
    assert not sponsor_df.empty

    # Step 4: Snapshot
    memo = generate_snapshot(config=config)
    assert isinstance(memo, str) and len(memo) > 20

    # Step 5: Export
    cfg = dict(config)
    cfg["export"] = {"csv": {"output_dir": str(tmp_path)}}
    path = export_deals_csv(config=cfg)
    assert os.path.exists(path)


def test_full_pipeline_with_synthetic(db, config, tmp_path):
    from ma.ingest.seed_real import seed_real_deals
    from ma.ingest.seed_synthetic import seed_synthetic_deals

    seed_real_deals(config)
    real_count = queries.get_deals_count()
    syn_inserted = seed_synthetic_deals(config, real_count)
    assert syn_inserted > 0

    total = queries.get_deals_count()
    assert total == real_count + syn_inserted

    # No null data_origin
    from ma.db.engine import get_connection
    conn = get_connection()
    nulls = conn.execute("SELECT COUNT(*) FROM deals WHERE data_origin IS NULL").fetchone()[0]
    assert nulls == 0


def test_flat_view_joins_correctly(db, config):
    from ma.ingest.seed_real import seed_real_deals
    seed_real_deals(config)
    df = queries.get_all_deals()
    # All rows should have sector_name (at least some)
    assert "sector_name" in df.columns
    # All rows have acquirer_name or None
    assert "acquirer_name" in df.columns


def test_filter_by_data_origin(db, config):
    from ma.ingest.seed_real import seed_real_deals
    from ma.ingest.seed_synthetic import seed_synthetic_deals
    seed_real_deals(config)
    existing = queries.get_deals_count()
    seed_synthetic_deals(config, existing)

    real_df = queries.get_all_deals({"data_origin": "real"})
    syn_df = queries.get_all_deals({"data_origin": "synthetic"})

    assert (real_df["data_origin"] == "real").all()
    assert (syn_df["data_origin"] == "synthetic").all()


def test_completeness_scores_computed(db, config):
    from ma.ingest.seed_real import seed_real_deals
    seed_real_deals(config)
    from ma.db.engine import get_connection
    conn = get_connection()
    result = conn.execute("SELECT COUNT(*) FROM deal_metadata WHERE completeness_score IS NOT NULL").fetchone()
    assert result[0] > 0
