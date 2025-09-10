import pandas as pd
from datetime import date
from clients import NSEClient
import os
import time
import duckdb
import argparse


def read_index_list(db_file: str = "data/db/stock.duckdb", exchange: str = "NSE", index_type: str | None = None):
    """
    Read index names from DuckDB `universe_indexes` for the given exchange and optional type.
    """
    try:
        con = duckdb.connect(database=db_file, read_only=True)
        try:
            if index_type:
                df = con.execute(
                    """
                    SELECT DISTINCT "Index"
                    FROM universe_indexes
                    WHERE COALESCE("Exchange", 'NSE') = ?
                      AND ("Type" = ?)
                    ORDER BY "Index"
                    """,
                    [exchange, index_type],
                ).df()
            else:
                df = con.execute(
                    """
                    SELECT DISTINCT "Index"
                    FROM universe_indexes
                    WHERE COALESCE("Exchange", 'NSE') = ?
                    ORDER BY "Index"
                    """,
                    [exchange],
                ).df()
        finally:
            con.close()
        indices = df['Index'].tolist()
        print(f"Found {len(indices)} indices for {exchange}{' / ' + index_type if index_type else ''}")
        return indices
    except Exception as e:
        print(f"Error reading indexes from DuckDB ({db_file}): {e}")
        return []


def download_all_indices(indices, start_date, end_date, delay=1):
    """
    Download data for all indices with a delay between requests to avoid rate limiting.
    """
    # Create data directory if it doesn't exist
    os.makedirs("data/cache/index_history", exist_ok=True)
    client = NSEClient()
    
    successful_downloads = 0
    failed_downloads = []
    
    for i, index_name in enumerate(indices, 1):
        print(f"\n{'='*60}")
        print(f"Processing {i}/{len(indices)}: {index_name}")
        print(f"{'='*60}")
        
        filepath = f"data/cache/index_history/{index_name.replace(' ', '_')}.csv"
        
        try:
            client.download_index_data(
                symbol=index_name,
                from_date=start_date,
                to_date=end_date,
                filename=filepath
            )
            successful_downloads += 1
            print(f"✓ Successfully downloaded data for {index_name}")
        except (FileNotFoundError, KeyError) as e:
            failed_downloads.append(index_name)
            print(f"✗ Failed to process data for {index_name}: {e}")
        except Exception as e:
            failed_downloads.append(index_name)
            print(f"✗ An unexpected error occurred for {index_name}: {e}")
        
        # Add delay between requests to avoid rate limiting
        if i < len(indices):  # Don't delay after the last request
            print(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)
    
    print(f"\n{'='*60}")
    print(f"DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total indices processed: {len(indices)}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {len(failed_downloads)}")
    if failed_downloads:
        print(f"Failed indices: {', '.join(failed_downloads)}")
    if len(indices) > 0:
        print(f"Success rate: {(successful_downloads/len(indices)*100):.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download historical index data for the universe or a filtered set.')
    parser.add_argument('--db-file', default='data/db/stock.duckdb', help='Path to DuckDB database file')
    parser.add_argument('--exchange', default='NSE', help='Exchange filter for universe (default: NSE)')
    parser.add_argument('--type', dest='index_type', help='Optional index type filter (e.g., "Sectoral")')
    parser.add_argument('--delay', type=int, default=2, help='Delay between requests in seconds')
    args = parser.parse_args()

    # Get list of indices from DB
    indices_to_download = read_index_list(db_file=args.db_file, exchange=args.exchange, index_type=args.index_type)
    
    if not indices_to_download:
        print("No indices found. Exiting.")
        exit(1)
    
    # Set date range
    start_date = date(2020, 1, 1)
    end_date = date.today()
    
    print(f"Date range: {start_date} to {end_date}")
    
    # Download data for all indices
    download_all_indices(indices_to_download, start_date, end_date, delay=args.delay) 