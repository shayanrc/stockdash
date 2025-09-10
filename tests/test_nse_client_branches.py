import os
import pandas as pd
import pytest
from datetime import date

from clients import NSEClient


def test_download_stock_data_skips_when_csv_has_today(monkeypatch: pytest.MonkeyPatch, tmp_path):
    # Prepare existing CSV that already has today's date
    csv_path = tmp_path / "ABC.csv"
    today = date.today()
    seed = pd.DataFrame({
        "DATE": [today.isoformat(), (today.replace(day=today.day - 1) if today.day > 1 else today.isoformat())],
        "OPEN": [10.0, 9.0],
        "HIGH": [11.0, 10.0],
        "LOW": [9.0, 8.5],
        "CLOSE": [10.5, 9.5],
        "VWAP": [10.4, 9.4],
        "VOLUME": [1000, 900],
        "VALUE": [10000, 9000],
        "NOOFTRADES": [10, 9],
        "SYMBOL": ["ABC", "ABC"],
        "SERIES": ["EQ", "EQ"],
    })
    seed.to_csv(csv_path, index=False)

    # Ensure primary path is NOT called
    import clients.nse_client as nse_client_module
    def raising_stock_df(*args, **kwargs):
        raise AssertionError("primary stock_df should not be called when CSV has today")
    monkeypatch.setattr(nse_client_module.NSEClient, "_get_stock_df", lambda self: raising_stock_df)

    client = NSEClient(db_file=None)
    out = client.download_stock_data(
        symbol="ABC",
        from_date=today,
        to_date=today,
        filename=str(csv_path),
    )

    assert len(out) == len(seed)
    assert pd.to_datetime(out["DATE"]).dt.date.max() == today


def test_download_stock_data_timeout_fallback_to_nselib_with_chunking(monkeypatch: pytest.MonkeyPatch, tmp_path):
    # Force primary jugaad_data to timeout, triggering nselib fallback
    import clients.nse_client as nse_client_module

    def timeout_stock_df(*args, **kwargs):
        raise TimeoutError("simulated timeout")

    class DummyCapitalMarket:
        @staticmethod
        def price_volume_data(symbol, from_date, to_date):
            # Return two rows per chunk (start and end)
            return pd.DataFrame({
                "Date": [from_date, to_date],
                "Open Price": [10.0, 20.0],
                "High Price": [11.0, 21.0],
                "Low Price": [9.0, 19.0],
                "Prev Close": [9.5, 19.5],
                "LTP": [10.2, 20.2],
                "Close Price": [10.1, 20.1],
                "Avg Price": [10.05, 20.05],
                "Total Traded Quantity": [1000, 2000],
                "Turnover₹": ["1,000.5", "2,000.5"],
                "No. of Trades": [10, 20],
                "Series": ["EQ", "EQ"],
            })

    monkeypatch.setattr(nse_client_module.NSEClient, "_get_stock_df", lambda self: timeout_stock_df)
    monkeypatch.setattr(nse_client_module.NSEClient, "_get_capital_market", lambda self: DummyCapitalMarket)

    client = NSEClient(db_file=None, chunk_days=30)

    out_file = tmp_path / "ABC.csv"
    out = client.download_stock_data(
        symbol="ABC",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 2, 15),  # spans >30 days to create 2 chunks
        filename=str(out_file),
    )

    # Validate fallback path produced expected date coverage across chunks
    dates = pd.to_datetime(out["DATE"]).dt.date
    assert dates.min() == date(2024, 1, 1)
    assert dates.max() == date(2024, 2, 15)
    assert date(2024, 1, 31) in set(dates)
    assert set(["OPEN", "HIGH", "LOW", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"]).issubset(out.columns)
    assert os.path.exists(out_file)


def test_download_stock_data_keyerror_ch_skips_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path):
    import clients.nse_client as nse_client_module

    def keyerror_stock_df(*args, **kwargs):
        raise KeyError("CH_SYMBOL not found")

    monkeypatch.setattr(nse_client_module.NSEClient, "_get_stock_df", lambda self: keyerror_stock_df)

    client = NSEClient(db_file=None)
    out_path = tmp_path / "XYZ.csv"
    out = client.download_stock_data(
        symbol="XYZ",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 3),
        filename=str(out_path),
    )

    assert out.empty
    assert os.path.exists(out_path)


def test_fetch_equity_history_inverted_range_returns_empty():
    client = NSEClient()
    out = client._fetch_equity_history_nselib(
        symbol="ABC",
        from_date=date(2024, 1, 2),
        to_date=date(2024, 1, 1),
    )
    assert out.empty
    assert out.columns.tolist() == [
        "DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"
    ]


def test_download_index_data_default_filename_and_sanitization(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture):
    class DummyCapitalMarket:
        @staticmethod
        def index_data(symbol, from_date, to_date):
            return pd.DataFrame({
                "TIMESTAMP": ["01-01-2024", "02-01-2024"],
                "CLOSE_INDEX_VAL": [100.0, 101.0],
                "OPEN_INDEX_VAL": [99.0, 100.0],
            })

    import clients.nse_client as nse_client_module
    monkeypatch.setattr(nse_client_module.NSEClient, "_get_capital_market", lambda self: DummyCapitalMarket)

    client = NSEClient()

    # Use CWD as tmp_path and let method choose default filename
    monkeypatch.chdir(tmp_path)
    client.download_index_data(
        symbol="NIFTY 50",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 3),
        filename=None,
    )

    # Ensure a sanitized file exists
    files = [f for f in os.listdir(tmp_path) if f.endswith(".csv")]
    assert len(files) == 1
    fname = files[0]
    assert "NIFTY_50" in fname and "2024-01-01_2024-01-03" in fname

    df = pd.read_csv(tmp_path / fname)
    assert "Date" in df.columns and "Close" in df.columns


def test_download_index_data_empty_raises(monkeypatch: pytest.MonkeyPatch, tmp_path):
    class DummyCapitalMarket:
        @staticmethod
        def index_data(symbol, from_date, to_date):
            return pd.DataFrame({})

    import clients.nse_client as nse_client_module
    monkeypatch.setattr(nse_client_module.NSEClient, "_get_capital_market", lambda self: DummyCapitalMarket)

    client = NSEClient()

    with pytest.raises(FileNotFoundError):
        client.download_index_data(
            symbol="NIFTY 50",
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 3),
            filename=str(tmp_path / "nifty.csv"),
        ) 