import argparse
import os
from datetime import date
import duckdb
from lib.clients.nse_client import download_stock_data

CACHE_STOCK_DIR = 'data/cache/price_history'

def read_universe_symbols(db_file: str, exchange: str = 'NSE'):
	con = duckdb.connect(database=db_file, read_only=True)
	try:
		df = con.execute(
			"""
			SELECT DISTINCT "Symbol"
			FROM universe_stocks
			WHERE COALESCE("Exchange", 'NSE') = ?
			ORDER BY "Symbol"
			""",
			[exchange],
		).df()
		return df['Symbol'].tolist()
	finally:
		con.close()


def main(db_file: str, exchange: str, start: date, end: date, delay: int, limit: int | None):
	os.makedirs(CACHE_STOCK_DIR, exist_ok=True)
	symbols = read_universe_symbols(db_file=db_file, exchange=exchange)
	if not symbols:
		print(f"No symbols found in universe for exchange {exchange}")
		return
	if limit is not None:
		symbols = symbols[:max(0, limit)]
	for i, symbol in enumerate(symbols, 1):
		print(f"\n{'='*60}\nProcessing {i}/{len(symbols)}: {symbol}\n{'='*60}")
		filepath = os.path.join(CACHE_STOCK_DIR, f"{symbol}.csv")
		try:
			download_stock_data(symbol=symbol, from_date=start, to_date=end, filename=filepath)
		except Exception as e:
			print(f"Failed {symbol}: {e}")
			continue
		if i < len(symbols) and delay > 0:
			import time
			time.sleep(delay)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Fetch stock price history to cache directory.')
	parser.add_argument('--db-file', default='data/db/stock.duckdb', help='Path to DuckDB database file')
	parser.add_argument('--exchange', default='NSE', help='Exchange filter for universe (default: NSE)')
	parser.add_argument('--start', default='2020-01-01', help='Start date YYYY-MM-DD')
	parser.add_argument('--end', default=date.today().strftime('%Y-%m-%d'), help='End date YYYY-MM-DD')
	parser.add_argument('--delay', type=int, default=2, help='Delay between requests (seconds)')
	parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols (for smoke runs)')
	args = parser.parse_args()
	start_date = date.fromisoformat(args.start)
	end_date = date.fromisoformat(args.end)
	main(db_file=args.db_file, exchange=args.exchange, start=start_date, end=end_date, delay=args.delay, limit=args.limit) 