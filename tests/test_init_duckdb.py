import duckdb
from pathlib import Path

import init_duckdb as idb


def get_column_names(con: duckdb.DuckDBPyConnection, table: str):
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # DuckDB PRAGMA table_info returns: (column_id, column_name, column_type, null, default)
    return [r[1] for r in rows]


def test_main_creates_all_schemas_and_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"

    # First run should create schemas (ensure parent directory exists)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    idb.main(db_file=str(db_path))

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Tables should exist
        tables = [
            "universe_stocks",
            "universe_indexes",
            "index_prices",
            "stock_prices",
        ]
        for t in tables:
            cols = get_column_names(con, t)
            assert len(cols) > 0, f"Expected columns for table {t}"

        # Check a couple of concrete schemas
        stock_cols = get_column_names(con, "stock_prices")
        assert stock_cols == [
            "date",
            "symbol",
            "exchange",
            "open",
            "high",
            "low",
            "prev_close",
            "ltp",
            "close",
            "vwap",
            "volume",
            "value",
            "trades",
        ]

        index_cols = get_column_names(con, "index_prices")
        assert index_cols == [
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
        ]
    finally:
        con.close()

    # Second run should succeed and keep schemas stable
    idb.main(db_file=str(db_path))

    con2 = duckdb.connect(str(db_path), read_only=True)
    try:
        stock_cols2 = get_column_names(con2, "stock_prices")
        assert stock_cols2 == [
            "date",
            "symbol",
            "exchange",
            "open",
            "high",
            "low",
            "prev_close",
            "ltp",
            "close",
            "vwap",
            "volume",
            "value",
            "trades",
        ]
    finally:
        con2.close() 