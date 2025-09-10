import os
import duckdb
from pathlib import Path

import populate_universe_duckdb as pud


def write_text(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_populate_universe_loads_stocks_and_indexes(tmp_path: Path):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"

    # Minimal universe stocks CSV
    stocks_csv = tmp_path / "data" / "universe" / "nse_nifty500.csv"
    write_text(
        stocks_csv,
        "\n".join([
            '"Company Name","Industry","Symbol","ISIN Code"',
            '"Acme Ltd","Capital Goods","ACME","INE000A00001"',
            '"Beta Corp","Financial Services","BETA","INE000B00002"',
        ])
    )

    # Minimal indexes CSV (one with explicit NSE, one empty to default)
    idx_csv = tmp_path / "data" / "universe" / "nse_indices.csv"
    write_text(
        idx_csv,
        "\n".join([
            '"Index","Exchange","Type"',
            '"NIFTY TEST","NSE","Benchmark"',
            '"NIFTY DEFAULT","","Sectoral"',
        ])
    )

    # Ensure DB parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    pud.main(db_file=str(db_path), csv_file=str(stocks_csv), indices_csv_file=str(idx_csv))

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        n_stocks = con.execute("SELECT COUNT(*) FROM universe_stocks").fetchone()[0]
        n_indexes = con.execute("SELECT COUNT(*) FROM universe_indexes").fetchone()[0]
        assert n_stocks == 2
        assert n_indexes == 2

        # Verify stock fields mapped and default Exchange
        rows = con.execute(
            'SELECT "Company Name", "Industry", "Symbol", "Exchange", "code" FROM universe_stocks ORDER BY "Symbol"'
        ).fetchall()
        assert rows[0][2] == "ACME"
        assert rows[0][3] == "NSE"
        assert rows[0][4] == "INE000A00001"

        # Verify index defaulted Exchange for empty value
        ex_default = con.execute(
            'SELECT "Exchange" FROM universe_indexes WHERE "Index" = ?',["NIFTY DEFAULT"]
        ).fetchone()[0]
        assert ex_default == "NSE"
    finally:
        con.close()


def test_populate_universe_handles_missing_indices_csv(tmp_path: Path, capsys):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"

    # Only stocks CSV present
    stocks_csv = tmp_path / "data" / "universe" / "nse_nifty500.csv"
    write_text(
        stocks_csv,
        "\n".join([
            '"Company Name","Industry","Symbol","ISIN Code"',
            '"Gamma Ltd","IT","GAMMA","INE000C00003"',
        ])
    )

    missing_idx = tmp_path / "data" / "universe" / "missing_indices.csv"

    # Ensure DB parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    pud.main(db_file=str(db_path), csv_file=str(stocks_csv), indices_csv_file=str(missing_idx))

    out = capsys.readouterr().out
    assert f"Indexes CSV not found: {missing_idx}" in out

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        n_stocks = con.execute("SELECT COUNT(*) FROM universe_stocks").fetchone()[0]
        assert n_stocks == 1
        # universe_indexes table should not exist when indices CSV is missing
        import pytest
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT COUNT(*) FROM universe_indexes").fetchone()
    finally:
        con.close() 