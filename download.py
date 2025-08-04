import os
from datetime import date, timedelta
from jugaad_data.nse import stock_df
import pandas as pd
from nselib import capital_market

def download_stock_data(symbol, from_date, to_date, filename=None):
    """
    Download historical stock data for a given symbol and date range.
    If a filename is provided and the file exists, it will only download the missing data.
    """
    if filename is None:
        filename = f"{symbol}_data_{from_date}_{to_date}.csv"

    print(f"Downloading historical data for {symbol} from {from_date} to {to_date}")
    
    existing_df = None
    if filename and os.path.exists(filename):
        print(f"File '{filename}' found. Reading existing data.")
        existing_df = pd.read_csv(filename)
        existing_df['DATE'] = pd.to_datetime(existing_df['DATE']).dt.date

    
    new_data_list = []
    

    if existing_df is not None and not existing_df.empty:
        min_date_in_file = existing_df['DATE'].min()
        max_date_in_file = existing_df['DATE'].max()
        
        print(f"Date range in file: {min_date_in_file} to {max_date_in_file}")

        # Check for data to download before the existing data
        if from_date < min_date_in_file:
            print(f"Fetching data from {from_date} to {min_date_in_file - timedelta(days=1)}")
            try:
                before_df = stock_df(symbol=symbol, from_date=from_date, to_date=min_date_in_file - timedelta(days=1), series="EQ")
                new_data_list.append(before_df)
            except Exception as e:
                print(f"Error downloading data for range {from_date} to {min_date_in_file - timedelta(days=1)}: {e}")

        # Check for data to download after the existing data
        if to_date > max_date_in_file:
            print(f"Fetching data from {max_date_in_file + timedelta(days=1)} to {to_date}")
            try:
                after_df = stock_df(symbol=symbol, from_date=max_date_in_file + timedelta(days=1), to_date=to_date, series="EQ")
                new_data_list.append(after_df)
            except Exception as e:
                print(f"Error downloading data for range {max_date_in_file + timedelta(days=1)} to {to_date}: {e}")

    else:
        # File doesn't exist or is empty, download the full range
        print("No existing data found. Downloading full date range.")
        try:
            full_df = stock_df(symbol=symbol, from_date=from_date, to_date=to_date, series="EQ")
            new_data_list.append(full_df)
        except Exception as e:
            print(f"Error downloading data for range {from_date} to {to_date}: {e}")


    if not new_data_list and existing_df is not None:
        print("No new data to download. File is already up-to-date for the given range.")
        return existing_df

    all_data = [existing_df] if existing_df is not None else []
    all_data.extend(new_data_list)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df['DATE'] = pd.to_datetime(combined_df['DATE']).dt.date
    combined_df = combined_df.drop_duplicates(subset=['DATE'], keep='last')
    combined_df = combined_df.sort_values(by='DATE', ascending=False)
    
    # Display basic information
    print(f"\nData shape: {combined_df.shape}")
    print(f"Columns: {list(combined_df.columns)}")
    print("\nFirst 5 rows:")
    print(combined_df.head())
    
    print("\nLast 5 rows:")
    print(combined_df.tail())
    
    # Save to CSV file
    combined_df.to_csv(filename, index=False)
    print(f"\nData saved to: {filename}")
    
    # Display some statistics
    print("\nBasic statistics:")
    print(f"Date range: {combined_df['DATE'].min()} to {combined_df['DATE'].max()}")
    print(f"Number of trading days: {len(combined_df)}")
    print(f"Average volume: {combined_df['VOLUME'].mean():,.0f}")
    print(f"Average close price: ₹{combined_df['CLOSE'].mean():.2f}")
    
    return combined_df


def download_index_data(symbol, from_date, to_date, filename=None):
    """
    Download historical index data for a given symbol and date range using nselib.
    If a filename is provided and the file exists, it will only download the missing data.
    """
    if filename is None:
        filename = f"{symbol}_data_{from_date}_{to_date}.csv"

    # In index names, spaces are there, which are not good for filenames.
    filename = filename.replace(" ", "_")

    print(f"Downloading historical data for {symbol} from {from_date} to {to_date}")
    
    from_date_str = from_date.strftime('%d-%m-%Y')
    to_date_str = to_date.strftime('%d-%m-%Y')

    try:
        df = capital_market.index_data(symbol, from_date=from_date_str, to_date=to_date_str)
        if df.empty:
            raise FileNotFoundError(f"No data available for {symbol} in the given date range.")
        
        df = df.rename(columns={'TIMESTAMP': 'Date', 'CLOSE_INDEX_VAL': 'Close'})
        
        df.to_csv(filename, index=False)
        print(f"\nData saved to: {filename}")
        
        # Display some statistics
        print("\nBasic statistics:")
        df['Date'] = pd.to_datetime(df['Date'])
        print(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
        print(f"Number of trading days: {len(df)}")
        print(f"Average close price: ₹{df['Close'].mean():.2f}")

    except Exception as e:
        print(f"Error downloading data for {symbol}: {e}")
        raise


