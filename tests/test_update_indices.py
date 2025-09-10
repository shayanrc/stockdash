import duckdb
import pandas as pd
from pathlib import Path

import update_indices as ui


def create_db_with_indexes(db_path: Path, indexes: list[tuple[str, str, str | None]]):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_indexes (
          "Index"    VARCHAR NOT NULL,
          "Exchange" VARCHAR DEFAULT 'NSE',
          "Type"     VARCHAR,
          PRIMARY KEY ("Index", "Exchange")
        );
        """
    )
    for idx, exch, typ in indexes:
        con.execute(
            'INSERT OR REPLACE INTO universe_indexes ("Index","Exchange","Type") VALUES (?,?,?)',
            [idx, exch, typ]
        )
    con.close()


essential_cols = ["TIMESTAMP", "CLOSE_INDEX_VAL", "OPEN_INDEX_VAL"]


def test_read_index_list_filters_and_error(tmp_path: Path):
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    create_db_with_indexes(db_path, [
        ("NIFTY A", "NSE", "Benchmark"),
        ("NIFTY B", "NSE", "Sectoral"),
        ("NIFTY C", "BSE", "Benchmark"),
    ])

    # No type filter
    out = ui.read_index_list(db_file=str(db_path), exchange="NSE", index_type=None)
    assert out == ["NIFTY A", "NIFTY B"]

    # With type filter
    out2 = ui.read_index_list(db_file=str(db_path), exchange="NSE", index_type="Sectoral")
    assert out2 == ["NIFTY B"]

    # Error path
    missing_db = tmp_path / "missing.duckdb"
    out3 = ui.read_index_list(db_file=str(missing_db), exchange="NSE", index_type=None)
    assert out3 == []


def test_download_all_indices_happy_path(tmp_path: Path, monkeypatch):
    # Prepare fake client with count of calls and CSV writing
    calls: list[str] = []

    class FakeClient:
        def download_index_data(self, symbol, from_date, to_date, filename):
            calls.append(symbol)
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame({
                "TIMESTAMP": ["01-01-2024"],
                "CLOSE_INDEX_VAL": [100.0],
                "OPEN_INDEX_VAL": [99.0],
            })
            # The method under test writes via client and expects file creation only
            df.to_csv(filename, index=False)

    import update_indices as update_indices_module
    monkeypatch.setattr(update_indices_module, 'NSEClient', FakeClient)

    indices = ["IDX A", "IDX B"]

    # Run within tmp_path so relative paths land there
    monkeypatch.chdir(tmp_path)
    ui.download_all_indices(indices, start_date=pd.Timestamp("2024-01-01").date(), end_date=pd.Timestamp("2024-01-02").date(), delay=0)

    for name in indices:
        path = tmp_path / "data" / "cache" / "index_history" / f"{name.replace(' ', '_')}.csv"
        assert path.exists()

    assert calls == indices 