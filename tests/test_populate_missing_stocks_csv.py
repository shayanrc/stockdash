from pathlib import Path
import duckdb

import populate_universe_duckdb as pud


def write_text(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_populate_universe_handles_missing_stocks_csv(tmp_path: Path, capsys):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"

    # Only indices CSV present (stocks CSV missing)
    idx_csv = tmp_path / "data" / "universe" / "nse_indices.csv"
    write_text(
        idx_csv,
        "\n".join([
            '"Index","Exchange","Type"',
            '"NIFTY TEST","NSE","Benchmark"',
        ])
    )

    # Ensure DB parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Pass missing stocks CSV path explicitly
    missing_stocks = tmp_path / "data" / "universe" / "missing_nifty500.csv"
    pud.main(db_file=str(db_path), csv_file=str(missing_stocks), indices_csv_file=str(idx_csv))

    out = capsys.readouterr().out
    assert f"CSV not found: {missing_stocks}" in out

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # universe_stocks should not exist
        import pytest
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT COUNT(*) FROM universe_stocks").fetchone()

        # universe_indexes should be populated
        n_indexes = con.execute("SELECT COUNT(*) FROM universe_indexes").fetchone()[0]
        assert n_indexes == 1
    finally:
        con.close() 