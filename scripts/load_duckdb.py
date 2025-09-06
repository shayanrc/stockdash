import argparse
import subprocess

DEFAULT_DB = 'data/db/stock.duckdb'

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Load CSV histories into DuckDB (wrapper).')
	parser.add_argument('--db-file', default=DEFAULT_DB, help='Path to DuckDB database file')
	args = parser.parse_args()
	cmd = ['python', 'load_to_duckdb.py', '--db-file', args.db_file]
	print('Running:', ' '.join(cmd))
	subprocess.check_call(cmd) 