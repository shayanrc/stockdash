import pandas as pd
from datetime import date
from download import download_index_data
import os
import time

def get_index_list():
    """
    Return a list of Nifty indices to download.
    """
    return [
        "NIFTY 50",
        "NIFTY NEXT 50",
        "NIFTY 100",
        "NIFTY 200",
        "NIFTY 500",
        "NIFTY MIDCAP 50",
        "NIFTY SMALLCAP 50",
        "NIFTY BANK",
        "NIFTY IT",
    ]

def download_all_indices(indices, start_date, end_date, delay=1):
    """
    Download data for all indices with a delay between requests to avoid rate limiting.
    """
    # Create data directory if it doesn't exist
    os.makedirs("data/index_history", exist_ok=True)
    
    successful_downloads = 0
    failed_downloads = []
    
    for i, index_name in enumerate(indices, 1):
        print(f"\n{'='*60}")
        print(f"Processing {i}/{len(indices)}: {index_name}")
        print(f"{'='*60}")
        
        filepath = f"data/index_history/{index_name.replace(' ', '_')}.csv"
        
        try:
            download_index_data(
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
    # Get list of indices
    indices_to_download = get_index_list()
    
    if not indices_to_download:
        print("No indices found. Exiting.")
        exit(1)
    
    # Set date range
    start_date = date(2020, 1, 1)
    end_date = date.today()
    
    print(f"Date range: {start_date} to {end_date}")
    
    # Download data for all indices
    download_all_indices(indices_to_download, start_date, end_date, delay=2) 