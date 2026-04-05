"""
Shared pytest fixtures for the M&A database test suite.
Uses an in-memory DuckDB instance for each test — no file I/O.
"""
import pytest
from ma.utils.config_loader import load_config
from ma.db import engine, schema, queries


@pytest.fixture(scope="function")
def config():
    return load_config("config.yaml")


@pytest.fixture(scope="function")
def db(tmp_path):
    """Fresh in-memory DuckDB for each test. Tears down after the test."""
    db_path = str(tmp_path / "test.duckdb")
    engine.init_db(db_path)
    schema.create_schema()
    yield
    engine.close_connection()


@pytest.fixture(scope="function")
def seeded_db(db, config):
    """DB with real + synthetic seeds loaded."""
    from ma.ingest.seed_real import seed_real_deals
    from ma.ingest.seed_synthetic import seed_synthetic_deals
    seed_real_deals(config)
    cnt = queries.get_deals_count()
    seed_synthetic_deals(config, cnt)
    yield
