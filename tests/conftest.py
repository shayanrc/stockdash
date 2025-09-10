import os
import sys
import types

# Ensure project root is on sys.path when running tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT not in sys.path:
	sys.path.insert(0, ROOT)

# Provide a minimal shim for nselib to avoid import errors in environments without it
if 'nselib' not in sys.modules:
	nselib = types.ModuleType('nselib')
	capital_market = types.ModuleType('capital_market')
	# Minimal placeholders; tests will monkeypatch as needed
	setattr(capital_market, 'index_data', lambda *args, **kwargs: None)
	setattr(capital_market, 'price_volume_data', lambda *args, **kwargs: None)
	setattr(nselib, 'capital_market', capital_market)
	sys.modules['nselib'] = nselib

# Provide a minimal shim for jugaad_data.nse.stock_df
if 'jugaad_data' not in sys.modules:
	jugaad_data = types.ModuleType('jugaad_data')
	sys.modules['jugaad_data'] = jugaad_data

# Ensure submodule jugaad_data.nse exists with stock_df symbol
if 'jugaad_data.nse' not in sys.modules:
	nse = types.ModuleType('jugaad_data.nse')
	setattr(nse, 'stock_df', lambda *args, **kwargs: None)
	sys.modules['jugaad_data.nse'] = nse 