import duckdb
import os
import argparse

DB_FILE = 'data/db/stock.duckdb'

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS universe_stocks (
  "Company Name" VARCHAR NOT NULL,
  "Industry"     VARCHAR,
  "Symbol"       VARCHAR NOT NULL,
  "Exchange"     VARCHAR DEFAULT 'NSE',
  "code"         VARCHAR,
  PRIMARY KEY ("Symbol", "Exchange")
);
"""

INDEX_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS index_prices (
  date DATE,
  symbol VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  turnover DOUBLE,
  PRIMARY KEY (date, symbol)
);
"""

STOCK_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS stock_prices (
  date DATE,
  symbol VARCHAR,
  exchange VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  prev_close DOUBLE,
  ltp DOUBLE,
  close DOUBLE,
  vwap DOUBLE,
  volume BIGINT,
  value DOUBLE,
  trades BIGINT,
  PRIMARY KEY (date, symbol, exchange)
);
"""

UNIVERSE_INDEXES_SQL = """
CREATE TABLE IF NOT EXISTS universe_indexes (
  "Index"    VARCHAR NOT NULL,
  "Exchange" VARCHAR DEFAULT 'NSE',
  "Type"     VARCHAR,
  PRIMARY KEY ("Index", "Exchange")
);
"""

def main(db_file=DB_FILE):
	con = duckdb.connect(database=db_file, read_only=False)
	try:
		con.execute(SCHEMA_SQL)
		con.execute(INDEX_PRICES_SQL)
		con.execute(STOCK_PRICES_SQL)
		con.execute(UNIVERSE_INDEXES_SQL)
		print("Schemas ensured in DuckDB")
	finally:
		con.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Create local DuckDB schema for universe_stocks.')
	parser.add_argument('--db-file', default=DB_FILE, help='Path to DuckDB database file')
	args = parser.parse_args()
	main(db_file=args.db_file) 