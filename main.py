"""
M&A Database — Orchestrator.
Runs the full pipeline: init DB → create schema → seed real → seed synthetic → export summary.
This file orchestrates only. All logic lives in ma/ modules.
"""
from ma.utils.config_loader import load_config
from ma.db.engine import init_db, close_connection
from ma.db.schema import create_schema
from ma.db import queries
from ma.ingest.seed_real import seed_real_deals
from ma.ingest.seed_synthetic import seed_synthetic_deals


def main():
    config = load_config("config.yaml")

    print("Initializing database...")
    init_db(config["database"]["path"])
    create_schema()

    # Check if already seeded (idempotent)
    existing_count = queries.get_deals_count()
    print(f"Existing deals in DB: {existing_count}")

    min_expected = config["seed"]["min_real_count"] + config["seed"]["synthetic_min"]
    if existing_count == 0:
        print("Seeding real deals...")
        real_inserted = seed_real_deals(config)
        print(f"  → {real_inserted} real deals inserted.")

        existing_count = queries.get_deals_count()
        print("Seeding synthetic deals...")
        syn_inserted = seed_synthetic_deals(config, existing_count)
        print(f"  → {syn_inserted} synthetic deals inserted.")
    elif existing_count < min_expected:
        print(f"⚠️  WARNING: Database has {existing_count} deals but expected at least {min_expected}.")
        print("   The database may be partially seeded or corrupt. Consider deleting")
        print("   data/ma_database.duckdb and re-running main.py to reseed from scratch.")
    else:
        print("Database already seeded. Skipping seed step.")

    total = queries.get_deals_count()
    kpis = queries.get_kpi_summary()
    print(f"\nDatabase ready: {total} total deals")
    print(f"  Real:      {kpis.get('real_deals', 0)}")
    print(f"  Synthetic: {kpis.get('synthetic_deals', 0)}")
    print(f"  Total deal value: ${kpis.get('total_deal_value_usd', 0)/1000:.1f}B")
    print(f"  Median EV/EBITDA: {kpis.get('median_ev_to_ebitda', 'N/A'):.1f}x"
          if kpis.get("median_ev_to_ebitda") else "  Median EV/EBITDA: N/A")
    print("\nRun: streamlit run app/streamlit_app.py")

    close_connection()


if __name__ == "__main__":
    main()
