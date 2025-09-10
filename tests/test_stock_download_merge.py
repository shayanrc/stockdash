import os
import pandas as pd
import tempfile
from datetime import date

from download import NSEClient


class DummyCapitalMarket:
	@staticmethod
	def price_volume_data(symbol, from_date, to_date):
		# Not used in primary path (we use jugaad) but keep for fallback
		return pd.DataFrame({})


def dummy_stock_df(symbol, from_date, to_date, series):
	# Return 3 rows: from_date, from_date+1, to_date
	dates = pd.date_range(from_date, to_date, freq="D")
	data = {
		"DATE": dates.strftime("%Y-%m-%d"),
		"OPEN": [10.0 + i for i in range(len(dates))],
		"HIGH": [11.0 + i for i in range(len(dates))],
		"LOW": [9.0 + i for i in range(len(dates))],
		"CLOSE": [10.5 + i for i in range(len(dates))],
		"VWAP": [10.4 + i for i in range(len(dates))],
		"VOLUME": [1000 + i for i in range(len(dates))],
		"VALUE": [10000 + i for i in range(len(dates))],
		"NOOFTRADES": [10 + i for i in range(len(dates))],
	}
	return pd.DataFrame(data)


def test_download_stock_data_appends_after_existing_csv(monkeypatch):
	# Monkeypatch external deps
	import download as download_module
	monkeypatch.setattr(download_module, "stock_df", dummy_stock_df)
	monkeypatch.setattr(download_module, "capital_market", DummyCapitalMarket)

	# Build client with no DB
	client = NSEClient(db_file=None)

	with tempfile.TemporaryDirectory() as tmpdir:
		csv_path = os.path.join(tmpdir, "ABC.csv")
		# Seed existing CSV with up to 2024-01-02
		seed = pd.DataFrame({
			"DATE": ["2024-01-01", "2024-01-02"],
			"OPEN": [10.0, 11.0],
			"HIGH": [11.0, 12.0],
			"LOW": [9.0, 10.0],
			"CLOSE": [10.5, 11.5],
			"VWAP": [10.4, 11.4],
			"VOLUME": [1000, 1001],
			"VALUE": [10000, 10001],
			"NOOFTRADES": [10, 11],
			"SYMBOL": ["ABC", "ABC"],
			"SERIES": ["EQ", "EQ"],
		})
		seed.to_csv(csv_path, index=False)

		# Now request range ending 2024-01-04; should append 01-03 and 01-04
		out = client.download_stock_data(
			symbol="ABC",
			from_date=date(2024, 1, 1),
			to_date=date(2024, 1, 4),
			filename=csv_path,
		)

		# Should contain 4 unique dates and be sorted desc in file
		assert len(out["DATE"].unique()) == 4
		assert out["DATE"].min().isoformat() == "2024-01-01"
		assert out["DATE"].max().isoformat() == "2024-01-04"


def test_download_stock_data_uses_db_max_date(monkeypatch, tmp_path):
	# Emulate DB by creating a temporary DuckDB with stock_prices
	import duckdb
	db_file = tmp_path / "test.duckdb"
	con = duckdb.connect(str(db_file), read_only=False)
	try:
		con.execute("CREATE TABLE stock_prices(date DATE, symbol VARCHAR);")
		con.execute("INSERT INTO stock_prices VALUES ('2024-01-03','ABC');")
	finally:
		con.close()

	# Monkeypatch network calls
	import download as download_module
	monkeypatch.setattr(download_module, "stock_df", dummy_stock_df)
	monkeypatch.setattr(download_module, "capital_market", DummyCapitalMarket)

	client = NSEClient(db_file=str(db_file))

	with tempfile.TemporaryDirectory() as tmpdir:
		csv_path = os.path.join(tmpdir, "ABC.csv")
		# Empty or missing CSV; should continue from DB date + 1 (2024-01-04)
		out = client.download_stock_data(
			symbol="ABC",
			from_date=date(2024, 1, 1),
			to_date=date(2024, 1, 5),
			filename=csv_path,
		)
		# Only dates 2024-01-04 and 2024-01-05 are fetched
		assert out["DATE"].min().isoformat() == "2024-01-04"
		assert out["DATE"].max().isoformat() == "2024-01-05" 