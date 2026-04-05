"""
DuckDB connection manager — single abstraction layer for all database access.
Using DuckDB: columnar, fast for aggregation, single-file, no server required.
All callers go through get_connection(); no raw duckdb.connect() elsewhere.
"""
import duckdb
from pathlib import Path
from typing import Optional


_connection: Optional[duckdb.DuckDBPyConnection] = None
_db_path: Optional[str] = None


def init_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Initialize the database connection. Creates the .duckdb file if it doesn't exist.
    Call once at application startup with the path from config.
    """
    global _connection, _db_path
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    _db_path = db_path
    _connection = duckdb.connect(db_path)
    return _connection


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the active DuckDB connection. Raises if init_db() was not called first."""
    if _connection is None:
        raise RuntimeError("Database not initialized. Call init_db(path) first.")
    return _connection


def close_connection() -> None:
    """Cleanly close the database connection."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def get_db_path() -> Optional[str]:
    """Return the path of the currently connected database file."""
    return _db_path
