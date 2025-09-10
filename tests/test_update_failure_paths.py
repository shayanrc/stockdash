import os
import pandas as pd
from datetime import date
from pathlib import Path
import pytest

import update_indices as ui
import update_stocks as us


def test_update_indices_handles_specific_and_generic_errors(monkeypatch: pytest.MonkeyPatch, tmp_path):
    # Fake client raising different exceptions
    class FakeClient:
        def __init__(self):
            self.calls = []
        def download_index_data(self, symbol, from_date, to_date, filename):
            self.calls.append(symbol)
            if symbol == "IDX_OK":
                Path(filename).parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame({
                    "TIMESTAMP": ["01-01-2024"],
                    "CLOSE_INDEX_VAL": [100.0],
                    "OPEN_INDEX_VAL": [99.0],
                }).to_csv(filename, index=False)
            elif symbol == "IDX_FNFE":
                raise FileNotFoundError("no data for range")
            elif symbol == "IDX_KEY":
                raise KeyError("missing column")
            else:
                raise RuntimeError("unexpected")

    import update_indices as update_indices_module
    monkeypatch.setattr(update_indices_module, 'NSEClient', FakeClient)

    indices = ["IDX_OK", "IDX_FNFE", "IDX_KEY", "IDX_OTHER"]
    monkeypatch.chdir(tmp_path)

    ui.download_all_indices(indices, start_date=date(2024,1,1), end_date=date(2024,1,2), delay=0)

    out = Path(tmp_path / "data" / "cache" / "index_history" / "IDX_OK.csv")
    assert out.exists()


def test_update_stocks_failure_summary(monkeypatch: pytest.MonkeyPatch, tmp_path):
    # Fake client: one ok, one fails
    class FakeClient:
        def __init__(self, db_file=None):
            self.db_file = db_file
        def download_stock_data(self, symbol, from_date, to_date, filename):
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            if symbol == "GOOD":
                pd.DataFrame({
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
                }).to_csv(filename, index=False)
            else:
                raise RuntimeError("boom")

    import update_stocks as update_stocks_module
    monkeypatch.setattr(update_stocks_module, 'NSEClient', FakeClient)

    monkeypatch.chdir(tmp_path)
    us.download_all_stocks(["GOOD", "BAD"], start_date=date(2024,1,1), end_date=date(2024,1,2), delay=0, db_file=None)

    ok_path = tmp_path / "data" / "cache" / "price_history" / "GOOD.csv"
    assert ok_path.exists() 