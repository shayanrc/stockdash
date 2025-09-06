import argparse
import os
import subprocess

DEFAULT_DB = 'data/db/stock.duckdb'

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Initialize local DuckDB schemas (wrapper).')
	parser.add_argument('--db-file', default=DEFAULT_DB, help='Path to DuckDB database file')
	args = parser.parse_args()
	cmd = ['python', 'init_local_db.py', '--db-file', args.db_file]
	print('Running:', ' '.join(cmd))
	subprocess.check_call(cmd) 