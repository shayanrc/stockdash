import duckdb
import os
import argparse

DB_FILE = 'data/db/stock.duckdb'
CSV_FILE = 'data/universe/nse_nifty500.csv'
INDICES_CSV_FILE = 'data/universe/nse_indices.csv'

INSERT_STOCKS_SQL = """
INSERT OR REPLACE INTO universe_stocks ("Company Name","Industry","Symbol","Exchange","code")
SELECT "Company Name","Industry","Symbol", 'NSE' AS "Exchange", "ISIN Code" AS "code"
FROM read_csv(?, header = true, sample_size = -1);
"""

INSERT_INDEXES_SQL = """
INSERT OR REPLACE INTO universe_indexes ("Index", "Exchange", "Type")
SELECT "Index", COALESCE("Exchange", 'NSE') AS "Exchange", "Type"
FROM read_csv(?, header = true, sample_size = -1);
"""

def main(db_file=DB_FILE, csv_file=CSV_FILE, indices_csv_file=INDICES_CSV_FILE):
	if not os.path.exists(csv_file):
		print(f"CSV not found: {csv_file}")
	else:
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

			con.execute(INSERT_STOCKS_SQL, [csv_file])
			print("universe_stocks populated from CSV")
		finally:
			con.close()

	# Populate indexes if CSV exists
	if os.path.exists(indices_csv_file):
		con = duckdb.connect(database=db_file, read_only=False)
		try:
			con.execute("""
			CREATE TABLE IF NOT EXISTS universe_indexes (
			  "Index"    VARCHAR NOT NULL,
			  "Exchange" VARCHAR DEFAULT 'NSE',
			  "Type"     VARCHAR,
			  PRIMARY KEY ("Index", "Exchange")
			);
			""")

			con.execute(INSERT_INDEXES_SQL, [indices_csv_file])
			print("universe_indexes populated from CSV")
		finally:
			con.close()
	else:
		print(f"Indexes CSV not found: {indices_csv_file}")

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Populate universe tables from CSVs into DuckDB.')
	parser.add_argument('--db-file', default=DB_FILE, help='Path to DuckDB database file')
	parser.add_argument('--csv-file', default=CSV_FILE, help='Path to stock universe CSV')
	parser.add_argument('--indices-csv', default=INDICES_CSV_FILE, help='Path to indices universe CSV')
	args = parser.parse_args()
	main(db_file=args.db_file, csv_file=args.csv_file, indices_csv_file=args.indices_csv) 