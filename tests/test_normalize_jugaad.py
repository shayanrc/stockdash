import pandas as pd
from datetime import date

from download import NSEClient


def test_normalize_jugaad_stock_df_basic():
	client = NSEClient()
	input_df = pd.DataFrame({
		"DATE": ["2024-01-02", "2024-01-01"],
		"OPEN": [100.5, 99.0],
		"HIGH": [101.0, 100.0],
		"LOW": [99.5, 98.5],
		"CLOSE": [100.0, 99.5],
		"VWAP": [100.2, 99.7],
		"VOLUME": [100000, 120000],
		"VALUE": [1_000_000, 1_200_000],
		"NOOFTRADES": [500, 600],
	})

	out = client._normalize_jugaad_stock_df(input_df, symbol="ABC", series="EQ")

	expected_prefix = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"]
	assert list(out.columns)[:len(expected_prefix)] == expected_prefix
	# Sorted ascending by DATE
	assert out.iloc[0]["DATE"].isoformat() == "2024-01-01"
	assert out.iloc[-1]["DATE"].isoformat() == "2024-01-02"
	# SYMBOL/SERIES filled
	assert (out["SYMBOL"] == "ABC").all()
	assert (out["SERIES"] == "EQ").all()


def test_normalize_jugaad_stock_df_renames_and_drop_52w():
	client = NSEClient()
	input_df = pd.DataFrame({
		"DATE": ["2024-01-01"],
		"PREV. CLOSE": [98.0],
		"NO OF TRADES": [700],
		"52W H": [200.0],
		"52W L": [50.0],
	})

	out = client._normalize_jugaad_stock_df(input_df, symbol="XYZ", series=None)

	assert "PREVCLOSE" in out.columns
	assert "NOOFTRADES" in out.columns
	assert "52W H" not in out.columns and "52W L" not in out.columns
	# SERIES defaults to client default
	assert (out["SERIES"] == client.default_series).all() 