import duckdb
import pandas as pd
from pathlib import Path
import types
import builtins

import update_stocks as us


def create_db_with_universe(db_path: Path, symbols: list[str]):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_stocks (
          "Company Name" VARCHAR,
          "Industry"     VARCHAR,
          "Symbol"       VARCHAR NOT NULL,
          "Exchange"     VARCHAR DEFAULT 'NSE',
          "code"         VARCHAR,
          PRIMARY KEY ("Symbol", "Exchange")
        );
        """
    )
    for s in symbols:
        con.execute(
            'INSERT OR REPLACE INTO universe_stocks ("Company Name","Industry","Symbol","Exchange","code") VALUES (?,?,?,?,?)',
            [s + " Ltd", "Sector", s, 'NSE', None]
        )
    con.close()


def test_read_stock_list_returns_symbols(tmp_path: Path):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    create_db_with_universe(db_path, ["AAA", "BBB", "CCC"])

    out = us.read_stock_list(db_file=str(db_path), exchange="NSE")
    assert out == ["AAA", "BBB", "CCC"]


def test_read_stock_list_handles_error(tmp_path: Path, monkeypatch):
    # Point to a non-existent DB file to trigger exception path
    db_path = tmp_path / "data" / "db" / "missing.duckdb"
    out = us.read_stock_list(db_file=str(db_path), exchange="NSE")
    assert out == []


def test_download_all_stocks_happy_path(tmp_path: Path, monkeypatch):
    # Arrange a fake client that writes predictable CSVs and counts calls
    calls: list[str] = []

    class FakeClient:
        def __init__(self, db_file=None):
            self.db_file = db_file
        def download_stock_data(self, symbol, from_date, to_date, filename):
            calls.append(symbol)
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame({
                "DATE": ["2024-01-01"],
                "OPEN": [1],
                "HIGH": [1],
                "LOW": [1],
                "CLOSE": [1],
                "VWAP": [1],
                "VOLUME": [1],
                "VALUE": [1],
                "NOOFTRADES": [1],
                "SYMBOL": [symbol],
                "SERIES": ["EQ"],
            })
            df.to_csv(filename, index=False)
            return df

    # Monkeypatch NSEClient constructor used inside update_stocks
    import update_stocks as update_stocks_module
    monkeypatch.setattr(update_stocks_module, 'NSEClient', FakeClient)

    symbols = ["AAA", "BBB"]

    # Run within tmp_path as CWD so relative paths go there
    monkeypatch.chdir(tmp_path)
    us.download_all_stocks(symbols, start_date=pd.Timestamp("2024-01-01").date(), end_date=pd.Timestamp("2024-01-02").date(), delay=0, db_file=None)

    # Verify files written and calls made

    for s in symbols:
        f = tmp_path / "data" / "cache" / "price_history" / f"{s}.csv"
        assert f.exists()

    assert calls == symbols 