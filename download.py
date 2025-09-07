import os
from datetime import date, timedelta
import pandas as pd
from nselib import capital_market
import duckdb


def _fetch_equity_history_nselib(symbol: str, from_date: date, to_date: date, series: str = "EQ") -> pd.DataFrame:
    """
    Fetch historical equity data using nselib.capital_market.price_volume_data in safe chunks.
    Returns a dataframe normalized to columns: DATE, OPEN, HIGH, LOW, PREVCLOSE, LTP, CLOSE, VWAP, VOLUME, VALUE, NOOFTRADES, SYMBOL, SERIES
    """
    if from_date > to_date:
        return pd.DataFrame(columns=[
            "DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"
        ])

    def _normalize_colname(name: str) -> str:
        # Lowercase and keep only alphanumerics to match variants like 'Open Price', 'OpenPrice', 'OPEN'
        return ''.join(ch for ch in name.lower() if ch.isalnum())

    def _resolve_first(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
        if df is None or df.empty:
            return None
        normalized_cols = { _normalize_colname(c): c for c in df.columns }
        for cand in candidates:
            key = _normalize_colname(cand)
            if key in normalized_cols:
                return df[normalized_cols[key]]
        return None

    def _clean_numeric(s: pd.Series | None) -> pd.Series | None:
        if s is None:
            return None
        # Remove commas, currency symbols, and spaces; keep digits, sign, and dot
        s_str = s.astype(str).str.replace(r"[^0-9+\-\.]+", "", regex=True)
        return s_str

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

                # Resolve columns across common NSE/nselib variants
                date_s = _resolve_first(df, ['Date', 'DATE', 'Timestamp', 'TIMESTAMP'])
                if date_s is None:
                    start = end + timedelta(days=1)
                    continue

                open_s = _resolve_first(df, ['OpenPrice', 'Open Price', 'OPEN', 'Open'])
                high_s = _resolve_first(df, ['HighPrice', 'High Price', 'HIGH', 'High'])
                low_s = _resolve_first(df, ['LowPrice', 'Low Price', 'LOW', 'Low'])
                prevclose_s = _resolve_first(df, ['PrevClose', 'Previous Close', 'PREV_CLOSE'])
                ltp_s = _resolve_first(df, ['LastPrice', 'LTP', 'LAST_PRICE'])
                close_s = _resolve_first(df, ['ClosePrice', 'Close Price', 'CLOSE', 'Close'])
                vwap_s = _resolve_first(df, ['AveragePrice', 'VWAP', 'Avg Price'])
                volume_s = _resolve_first(df, [
                    'TotalTradedQuantity', 'Total Traded Quantity', 'TOTAL_TRD_QTY', 'VOLUME', 'Volume'
                ])
                value_s = _resolve_first(df, ['Turnover₹', 'Turnover (₹ Cr)', 'Turnover', 'VALUE'])
                trades_s = _resolve_first(df, ['No.ofTrades', 'No. of Trades', 'TRADES'])
                series_s = _resolve_first(df, ['Series', 'SERIES'])

                parsed_date = pd.to_datetime(date_s, errors='coerce').dt.date

                normalized = pd.DataFrame({
                    "DATE": parsed_date,
                    "OPEN": pd.to_numeric(_clean_numeric(open_s), errors='coerce') if open_s is not None else pd.NA,
                    "HIGH": pd.to_numeric(_clean_numeric(high_s), errors='coerce') if high_s is not None else pd.NA,
                    "LOW": pd.to_numeric(_clean_numeric(low_s), errors='coerce') if low_s is not None else pd.NA,
                    "PREVCLOSE": pd.to_numeric(_clean_numeric(prevclose_s), errors='coerce') if prevclose_s is not None else pd.NA,
                    "LTP": pd.to_numeric(_clean_numeric(ltp_s), errors='coerce') if ltp_s is not None else pd.NA,
                    "CLOSE": pd.to_numeric(_clean_numeric(close_s), errors='coerce') if close_s is not None else pd.NA,
                    "VWAP": pd.to_numeric(_clean_numeric(vwap_s), errors='coerce') if vwap_s is not None else pd.NA,
                    "VOLUME": pd.to_numeric(_clean_numeric(volume_s), errors='coerce') if volume_s is not None else pd.NA,
                    "VALUE": pd.to_numeric(_clean_numeric(value_s), errors='coerce') if value_s is not None else pd.NA,
                    "NOOFTRADES": pd.to_numeric(_clean_numeric(trades_s), errors='coerce') if trades_s is not None else pd.NA,
                })
                normalized["SYMBOL"] = symbol
                normalized["SERIES"] = series_s if series_s is not None else series

                normalized = normalized.dropna(subset=["DATE"])  # type: ignore[arg-type]
                all_chunks.append(normalized)
        except Exception as e:
            print(f"Error fetching equity history for {symbol} {start_str} to {end_str}: {e}")
        finally:
            start = end + timedelta(days=1)

    if not all_chunks:
        return pd.DataFrame(columns=[
            "DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"
        ])

    out = pd.concat(all_chunks, ignore_index=True)
    out["DATE"] = pd.to_datetime(out["DATE"], errors='coerce').dt.date
    out = out.dropna(subset=["DATE"])  # type: ignore[arg-type]
    out = out.drop_duplicates(subset=["DATE"], keep='last')
    out = out.sort_values(by="DATE").reset_index(drop=True)
    return out


def download_stock_data(symbol, from_date, to_date, filename=None, db_file=None):
    """
    Download historical stock data for a given symbol and date range.
    If a filename is provided and the file exists, it will only download the missing data.
    End date for existing data is read from DuckDB when available (preferred over CSV).
    """
    if filename is None:
        filename = f"{symbol}_data_{from_date}_{to_date}.csv"

    print(f"Downloading historical data for {symbol} from {from_date} to {to_date}")

    # Prefer existing end date from DuckDB over CSV
    db_max_date = None
    if db_file and os.path.exists(db_file):
        try:
            con = duckdb.connect(database=db_file, read_only=True)
            try:
                res = con.execute(
                    """
                    SELECT max(date) AS max_date
                    FROM stock_prices
                    WHERE symbol = ?
                    """,
                    [symbol],
                ).fetchone()
                if res and res[0] is not None:
                    db_max_date = res[0]
                    print(f"Existing end date (DB): {db_max_date}")
            finally:
                con.close()
        except Exception as e:
            print(f"Warning: could not read max(date) from DuckDB for {symbol}: {e}")

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

        # Use CSV for the beginning of history (if expanding backwards)
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

        # For the end date, prefer DB's max(date) if available
        effective_max_existing = db_max_date if db_max_date is not None else max_date_in_file
        if effective_max_existing is not None and to_date > effective_max_existing:
            print(f"Fetching data from {effective_max_existing + timedelta(days=1)} to {to_date}")
            try:
                after_df = _fetch_equity_history_nselib(
                    symbol=symbol,
                    from_date=effective_max_existing + timedelta(days=1),
                    to_date=to_date,
                    series="EQ",
                )
                new_data_list.append(after_df)
            except Exception as e:
                print(f"Error downloading data for range {effective_max_existing + timedelta(days=1)} to {to_date}: {e}")

    else:
        # No CSV present or it's empty; if DB has an end date, continue from there
        if db_max_date is not None:
            print(f"No existing CSV or CSV empty. Using DB end date {db_max_date} to continue.")
            try:
                from_after = db_max_date + timedelta(days=1)
                if from_after <= to_date:
                    after_df = _fetch_equity_history_nselib(
                        symbol=symbol,
                        from_date=from_after,
                        to_date=to_date,
                        series="EQ",
                    )
                    new_data_list.append(after_df)
                else:
                    print("No new dates to fetch after DB end date.")
            except Exception as e:
                print(f"Error downloading data for range {db_max_date + timedelta(days=1)} to {to_date}: {e}")
        else:
            print("No existing data found in CSV or DB. Downloading full date range.")
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


