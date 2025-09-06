import argparse
import subprocess

DEFAULT_DB = 'data/db/stock.duckdb'
DEFAULT_CSV = 'data/universe/nse_nifty500.csv'

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Populate universe_stocks from CSV (wrapper).')
	parser.add_argument('--db-file', default=DEFAULT_DB, help='Path to DuckDB database file')
	parser.add_argument('--csv-file', default=DEFAULT_CSV, help='Path to universe CSV')
	args = parser.parse_args()
	cmd = ['python', 'populate_local_db.py', '--db-file', args.db_file, '--csv-file', args.csv_file]
	print('Running:', ' '.join(cmd))
	subprocess.check_call(cmd) 