import pandas as pd
from datetime import date
from download import NSEClient
import os
import time
import duckdb
import argparse

def read_stock_list(db_file: str = "data/db/stock.duckdb", exchange: str = "NSE"):
    """
    Read stock symbols from DuckDB `universe_stocks` for the given exchange.
    """
    try:
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
        finally:
            con.close()
        symbols = df['Symbol'].tolist()
        print(f"Found {len(symbols)} stocks in the universe for {exchange}")
        return symbols
    except Exception as e:
        print(f"Error reading stock list from DuckDB ({db_file}): {e}")
        return []

def download_all_stocks(symbols, start_date, end_date, delay=1, db_file=None):
    """
    Download data for all stocks with a delay between requests to avoid rate limiting.
    """
    # Create data directory if it doesn't exist
    os.makedirs("data/cache/price_history", exist_ok=True)
    client = NSEClient(db_file=db_file)
    
    successful_downloads = 0
    failed_downloads = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n{'='*60}")
        print(f"Processing {i}/{len(symbols)}: {symbol}")
        print(f"{'='*60}")
        
        filepath = f"data/cache/price_history/{symbol}.csv"
        
        try:
            client.download_stock_data(
                symbol=symbol,
                from_date=start_date,
                to_date=end_date,
                filename=filepath,
            )
            successful_downloads += 1
            print(f"✓ Successfully downloaded data for {symbol}")
        except Exception as e:
            failed_downloads.append(symbol)
            print(f"✗ Failed to download data for {symbol}: {e}")
        
        # Add delay between requests to avoid rate limiting
        if i < len(symbols):  # Don't delay after the last request
            print(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)
    
    print(f"\n{'='*60}")
    print(f"DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total stocks processed: {len(symbols)}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {len(failed_downloads)}")
    if failed_downloads:
        print(f"Failed symbols: {', '.join(failed_downloads)}")
    print(f"Success rate: {(successful_downloads/len(symbols)*100):.1f}%")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download historical stock data for the universe or a single symbol.')
    parser.add_argument('--db-file', default='data/db/stock.duckdb', help='Path to DuckDB database file')
    parser.add_argument('--exchange', default='NSE', help='Exchange filter for universe (default: NSE)')
    parser.add_argument('--delay', type=int, default=2, help='Delay between requests in seconds')
    parser.add_argument('--symbol', help='Download only this single stock symbol (e.g., RELIANCE)')
    args = parser.parse_args()

    # Set date range
    start_date = date(2020, 1, 1)
    end_date = date.today()
    print(f"Date range: {start_date} to {end_date}")

    if args.symbol:
        # Single-symbol path
        symbol = args.symbol.strip().upper()
        os.makedirs("data/cache/price_history", exist_ok=True)
        filepath = f"data/cache/price_history/{symbol}.csv"
        print(f"Downloading single symbol: {symbol}")
        client = NSEClient(db_file=args.db_file)
        try:
            client.download_stock_data(
                symbol=symbol,
                from_date=start_date,
                to_date=end_date,
                filename=filepath,
            )
            print(f"✓ Successfully downloaded data for {symbol}")
        except Exception as e:
            print(f"✗ Failed to download data for {symbol}: {e}")
    else:
        # Multi-symbol path
        # Read stock symbols from DuckDB
        stock_symbols = read_stock_list(db_file=args.db_file, exchange=args.exchange)
        
        if not stock_symbols:
            print("No stock symbols found. Exiting.")
            exit(1)
        
        # Download data for all stocks
        download_all_stocks(stock_symbols, start_date, end_date, delay=args.delay, db_file=args.db_file)