import duckdb
import os
import argparse

DB_FILE = 'stock_data.db'
CSV_FILE = 'data/ind_nifty500list.csv'

INSERT_SQL = """
INSERT OR REPLACE INTO universe_stocks ("Company Name","Industry","Symbol","Exchange","code")
SELECT "Company Name","Industry","Symbol", 'NSE' AS "Exchange", "ISIN Code" AS "code"
FROM read_csv(?, header = true, sample_size = -1);
"""

def main(db_file=DB_FILE, csv_file=CSV_FILE):
	if not os.path.exists(csv_file):
		print(f"CSV not found: {csv_file}")
		return

	con = duckdb.connect(database=db_file, read_only=False)
	try:
		# Ensure target table exists
		con.execute("""
		CREATE TABLE IF NOT EXISTS universe_stocks (
		  "Company Name" VARCHAR NOT NULL,
		  "Industry"     VARCHAR,
		  "Symbol"       VARCHAR NOT NULL,
		  "Exchange"     VARCHAR DEFAULT 'NSE',
		  "code"         VARCHAR,
		  PRIMARY KEY ("Symbol", "Exchange")
		);
		""")

		con.execute(INSERT_SQL, [csv_file])
		print("universe_stocks populated from CSV")
	finally:
		con.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Populate universe_stocks from CSV into DuckDB.')
	parser.add_argument('--db-file', default=DB_FILE, help='Path to DuckDB database file')
	parser.add_argument('--csv-file', default=CSV_FILE, help='Path to stock universe CSV')
	args = parser.parse_args()
	main(db_file=args.db_file, csv_file=args.csv_file) 