import os
from datetime import date, timedelta
import pandas as pd
from nselib import capital_market


def _fetch_equity_history_nselib(symbol: str, from_date: date, to_date: date, series: str = "EQ") -> pd.DataFrame:
    """
    Fetch historical equity data using nselib.capital_market.price_volume_data in safe chunks.
    Returns a dataframe normalized to columns: DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL, SERIES
    """
    if from_date > to_date:
        return pd.DataFrame(columns=[
            "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL", "SERIES"
        ])

    # price_volume_data can limit ranges; use 60-day chunks conservatively
    chunk_days = 60
    all_chunks: list[pd.DataFrame] = []
    start = from_date
    while start <= to_date:
        end = min(start + timedelta(days=chunk_days - 1), to_date)
        start_str = start.strftime('%d-%m-%Y')
        end_str = end.strftime('%d-%m-%Y')
        try:
            raw_df = capital_market.price_volume_data(symbol, start_str, end_str)
            if raw_df is not None and not raw_df.empty:
                df = raw_df.copy()

                # Expected columns from nselib.price_volume_data
                # ['Symbol','Series','Date','PrevClose','OpenPrice','HighPrice','LowPrice','LastPrice','ClosePrice','AveragePrice','TotalTradedQuantity','Turnover₹','No.ofTrades']
                if 'Date' not in df.columns:
                    start = end + timedelta(days=1)
                    continue

                parsed_date = pd.to_datetime(df['Date'], errors='coerce').dt.date

                normalized = pd.DataFrame({
                    "DATE": parsed_date,
                    "OPEN": pd.to_numeric(df.get('OpenPrice'), errors='coerce'),
                    "HIGH": pd.to_numeric(df.get('HighPrice'), errors='coerce'),
                    "LOW": pd.to_numeric(df.get('LowPrice'), errors='coerce'),
                    "CLOSE": pd.to_numeric(df.get('ClosePrice'), errors='coerce'),
                    "VOLUME": pd.to_numeric(df.get('TotalTradedQuantity'), errors='coerce'),
                })
                normalized["SYMBOL"] = symbol
                # Prefer Series from response if present
                normalized["SERIES"] = df.get('Series', series)

                normalized = normalized.dropna(subset=["DATE"])  # type: ignore[arg-type]
                all_chunks.append(normalized)
        except Exception as e:
            print(f"Error fetching equity history for {symbol} {start_str} to {end_str}: {e}")
        finally:
            start = end + timedelta(days=1)

    if not all_chunks:
        return pd.DataFrame(columns=[
            "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL", "SERIES"
        ])

    out = pd.concat(all_chunks, ignore_index=True)
    out["DATE"] = pd.to_datetime(out["DATE"], errors='coerce').dt.date
    out = out.dropna(subset=["DATE"])  # type: ignore[arg-type]
    out = out.drop_duplicates(subset=["DATE"], keep='last')
    out = out.sort_values(by="DATE").reset_index(drop=True)
    return out


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

        if from_date < min_date_in_file:
            print(f"Fetching data from {from_date} to {min_date_in_file - timedelta(days=1)}")
            try:
                before_df = _fetch_equity_history_nselib(
                    symbol=symbol,
                    from_date=from_date,
                    to_date=min_date_in_file - timedelta(days=1),
                    series="EQ",
                )
                new_data_list.append(before_df)
            except Exception as e:
                print(f"Error downloading data for range {from_date} to {min_date_in_file - timedelta(days=1)}: {e}")

        if to_date > max_date_in_file:
            print(f"Fetching data from {max_date_in_file + timedelta(days=1)} to {to_date}")
            try:
                after_df = _fetch_equity_history_nselib(
                    symbol=symbol,
                    from_date=max_date_in_file + timedelta(days=1),
                    to_date=to_date,
                    series="EQ",
                )
                new_data_list.append(after_df)
            except Exception as e:
                print(f"Error downloading data for range {max_date_in_file + timedelta(days=1)} to {to_date}: {e}")

    else:
        print("No existing data found. Downloading full date range.")
        try:
            full_df = _fetch_equity_history_nselib(
                symbol=symbol,
                from_date=from_date,
                to_date=to_date,
                series="EQ",
            )
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
    
    print(f"\nData shape: {combined_df.shape}")
    print(f"Columns: {list(combined_df.columns)}")
    print("\nFirst 5 rows:")
    print(combined_df.head())
    
    print("\nLast 5 rows:")
    print(combined_df.tail())
    
    combined_df.to_csv(filename, index=False)
    print(f"\nData saved to: {filename}")
    
    print("\nBasic statistics:")
    print(f"Date range: {combined_df['DATE'].min()} to {combined_df['DATE'].max()}")
    if 'VOLUME' in combined_df.columns:
        print(f"Number of trading days: {len(combined_df)}")
        print(f"Average volume: {combined_df['VOLUME'].mean():,.0f}")
    if 'CLOSE' in combined_df.columns:
        print(f"Average close price: ₹{combined_df['CLOSE'].mean():.2f}")
    
    return combined_df


def download_index_data(symbol, from_date, to_date, filename=None):
    """
    Download historical index data for a given symbol and date range using nselib.
    If a filename is provided and the file exists, it will only download the missing data.
    """
    if filename is None:
        filename = f"{symbol}_data_{from_date}_{to_date}.csv"

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
        
        print("\nBasic statistics:")
        df['Date'] = pd.to_datetime(df['Date'])
        print(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
        print(f"Number of trading days: {len(df)}")
        print(f"Average close price: ₹{df['Close'].mean():.2f}")

    except Exception as e:
        print(f"Error downloading data for {symbol}: {e}")
        raise


