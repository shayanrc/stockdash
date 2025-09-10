import os
import pandas as pd
import tempfile
from datetime import date

from download import NSEClient


class DummyCapitalMarket:
	@staticmethod
	def index_data(symbol, from_date, to_date):
		# Simulate minimal DataFrame returned by nselib
		return pd.DataFrame({
			"TIMESTAMP": ["01-01-2024", "02-01-2024"],
			"CLOSE_INDEX_VAL": [100.0, 101.5],
			"OPEN_INDEX_VAL": [99.0, 100.5],
		})


def test_download_index_data_writes_csv_and_renames(monkeypatch):
	client = NSEClient()

	# Monkeypatch the capital_market module inside download module
	import download as download_module
	monkeypatch.setattr(download_module, "capital_market", DummyCapitalMarket)

	with tempfile.TemporaryDirectory() as tmpdir:
		outfile = os.path.join(tmpdir, "NIFTY_50.csv")
		client.download_index_data(
			symbol="NIFTY 50",
			from_date=date(2024, 1, 1),
			to_date=date(2024, 1, 2),
			filename=outfile,
		)

		assert os.path.exists(outfile)
		df = pd.read_csv(outfile)
		# Renamed columns
		assert "Date" in df.columns
		assert "Close" in df.columns
		# Values should match the dummy data after rename
		assert df.loc[0, "Close"] == 100.0 