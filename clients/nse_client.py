import os
from datetime import date, timedelta
import pandas as pd
import duckdb
from typing import Callable
import signal
from contextlib import contextmanager


class NSEClient:
    """
    Client for fetching NSE stock and index data with optional DuckDB awareness.

    - Use download_stock_data(...) to fetch/append normalized per-symbol CSVs (prefers DB end-date).
    - Use download_index_data(...) to fetch index history CSVs.
    """

    def __init__(
        self,
        db_file: str | None = None,
        exchange: str = "NSE",
        default_series: str = "EQ",
        chunk_days: int = 60,
        logger: Callable[[str], None] | None = None,
        timeout_seconds: int = 20,
    ) -> None:
        self.db_file = db_file
        self.exchange = exchange
        self.default_series = default_series
        self.chunk_days = chunk_days
        self._logger = logger
        self.timeout_seconds = timeout_seconds

    def _log(self, message: str) -> None:
        if self._logger is not None:
            self._logger(message)
        else:
            print(message)

    @contextmanager
    def _timeout(self, seconds: int, on_timeout_message: str):
        def _handle(signum, frame):
            raise TimeoutError(on_timeout_message)
        old = signal.signal(signal.SIGALRM, _handle)
        signal.alarm(max(1, seconds))
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old)

    # Dependency accessors (allow override/shim to redirect or tests to patch via wrapper)
    def _get_capital_market(self):
        from nselib import capital_market as _capital_market
        return _capital_market

    def _get_stock_df(self):
        from jugaad_data.nse import stock_df as _stock_df
        return _stock_df

    def _fetch_equity_history_nselib(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        series: str | None = None,
    ) -> pd.DataFrame:
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

        all_chunks: list[pd.DataFrame] = []
        start = from_date
        while start <= to_date:
            end = min(start + timedelta(days=self.chunk_days - 1), to_date)
            start_str = start.strftime('%d-%m-%Y')
            end_str = end.strftime('%d-%m-%Y')
            try:
                self._log(f"nselib chunk {symbol}: {start_str} → {end_str}")
                with self._timeout(self.timeout_seconds, f"nselib timeout for {symbol} {start_str}→{end_str}"):
                    raw_df = self._get_capital_market().price_volume_data(symbol, start_str, end_str)
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
                    normalized["SERIES"] = series_s if series_s is not None else (series or self.default_series)

                    normalized = normalized.dropna(subset=["DATE"])  # type: ignore[arg-type]
                    all_chunks.append(normalized)
            except TimeoutError as te:
                self._log(str(te))
            except Exception as e:
                self._log(f"Error fetching equity history for {symbol} {start_str} to {end_str}: {e}")
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

    def _normalize_jugaad_stock_df(self, df: pd.DataFrame, symbol: str, series: str | None = None) -> pd.DataFrame:
        """
        Minimal normalization for jugaad_data.nse.stock_df output:
        - Rename: 'PREV. CLOSE' -> 'PREVCLOSE', 'NO OF TRADES' -> 'NOOFTRADES'
        - Drop: '52W H', '52W L'
        - Ensure DATE is a date type
        - Ensure SYMBOL and SERIES present
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=[
                "DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"
            ])

        out = df.copy()

        # Minimal renames
        rename_map: dict[str, str] = {}
        if 'PREV. CLOSE' in out.columns:
            rename_map['PREV. CLOSE'] = 'PREVCLOSE'
        if 'NO OF TRADES' in out.columns:
            rename_map['NO OF TRADES'] = 'NOOFTRADES'
        if rename_map:
            out = out.rename(columns=rename_map)

        # Drop 52-week columns if present
        drop_cols = [c for c in ['52W H', '52W L'] if c in out.columns]
        if drop_cols:
            out = out.drop(columns=drop_cols)

        # Parse DATE to date
        if 'DATE' in out.columns:
            out['DATE'] = pd.to_datetime(out['DATE'], errors='coerce').dt.date

        # Ensure SYMBOL and SERIES present
        if 'SYMBOL' not in out.columns:
            out['SYMBOL'] = symbol
        if 'SERIES' not in out.columns:
            out['SERIES'] = series or self.default_series

        # Reorder to preferred order when available
        preferred = ["DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"]
        ordered = [c for c in preferred if c in out.columns]
        remaining = [c for c in out.columns if c not in ordered]
        normalized = out[ordered + remaining]

        normalized = normalized.dropna(subset=["DATE"])  # type: ignore[arg-type]
        normalized = normalized.drop_duplicates(subset=["DATE"], keep='last')
        normalized = normalized.sort_values(by="DATE").reset_index(drop=True)
        return normalized

    def download_stock_data(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        filename: str | None = None,
    ) -> pd.DataFrame:
        """
        Download historical stock data for a given symbol and date range.
        If a filename is provided and the file exists, it will only download the missing data.
        End date for existing data is read from DuckDB when available (preferred over CSV).
        """
        if filename is None:
            filename = f"{symbol}_data_{from_date}_{to_date}.csv"

        self._log(f"Downloading historical data for {symbol} from {from_date} to {to_date}")

        # Prefer existing end date from DuckDB over CSV
        db_max_date = None
        if self.db_file and os.path.exists(self.db_file):
            try:
                con = duckdb.connect(database=self.db_file, read_only=True)
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
                        self._log(f"Existing end date (DB): {db_max_date}")
                finally:
                    con.close()
            except Exception as e:
                self._log(f"Warning: could not read max(date) from DuckDB for {symbol}: {e}")

        existing_df = None
        if filename and os.path.exists(filename):
            self._log(f"File '{filename}' found. Reading existing data.")
            try:
                existing_df = pd.read_csv(filename)
                self._log(f"Existing rows in CSV: {0 if existing_df is None else len(existing_df)}")
                if 'DATE' in existing_df.columns:
                    existing_df['DATE'] = pd.to_datetime(existing_df['DATE']).dt.date
            except Exception as e:
                self._log(f"Warning: failed to read existing CSV '{filename}': {e}")
                existing_df = None

        # Early skip if already updated today (check DB and CSV)
        max_date_in_file = None
        if existing_df is not None and not existing_df.empty and 'DATE' in existing_df.columns:
            try:
                max_date_in_file = existing_df['DATE'].max()
            except Exception:
                max_date_in_file = None
        today = date.today()
        if (db_max_date is not None and db_max_date == today) or (max_date_in_file is not None and max_date_in_file == today):
            self._log(f"Already up-to-date for today ({today}). Skipping {symbol}.")
            return existing_df if existing_df is not None else pd.DataFrame()

        new_data_list: list[pd.DataFrame] = []

        def fetch_with_primary_and_fallback(f: date, t: date) -> pd.DataFrame:
            # Primary: jugaad_data.nse.stock_df (fast)
            try:
                self._log(f"jugaad_data stock_df {symbol}: {f} → {t}")
                with self._timeout(self.timeout_seconds, f"jugaad_data timeout for {symbol} {f}→{t}"):
                    jd = self._get_stock_df()(symbol=symbol, from_date=f, to_date=t, series=self.default_series)
                return self._normalize_jugaad_stock_df(jd, symbol=symbol, series=self.default_series)
            except TimeoutError as te:
                self._log(str(te))
                return self._fetch_equity_history_nselib(symbol=symbol, from_date=f, to_date=t, series=self.default_series)
            except Exception as e:
                # Treat CH_* KeyError as 'no data' (pre-listing or unavailable); skip fallback to nselib
                if isinstance(e, KeyError) and ("CH_" in str(e)):
                    self._log(f"No jugaad-data data for {symbol} {f} to {t} (likely pre-listing). Skipping this range.")
                    return pd.DataFrame(columns=[
                        "DATE", "OPEN", "HIGH", "LOW", "PREVCLOSE", "LTP", "CLOSE", "VWAP", "VOLUME", "VALUE", "NOOFTRADES", "SYMBOL", "SERIES"
                    ])
                self._log(f"Primary fetch via jugaad-data failed for {symbol} {f} to {t}: {e}. Falling back to nselib.")
                return self._fetch_equity_history_nselib(symbol=symbol, from_date=f, to_date=t, series=self.default_series)

        if existing_df is not None and not existing_df.empty:
            min_date_in_file = existing_df['DATE'].min()
            max_date_in_file = existing_df['DATE'].max()

            self._log(f"Date range in file: {min_date_in_file} to {max_date_in_file}")

            # Use CSV for the beginning of history (if expanding backwards)
            if from_date < min_date_in_file:
                self._log(f"Fetching data from {from_date} to {min_date_in_file - timedelta(days=1)}")
                try:
                    before_df = fetch_with_primary_and_fallback(
                        f=from_date,
                        t=min_date_in_file - timedelta(days=1),
                    )
                    new_data_list.append(before_df)
                except Exception as e:
                    self._log(f"Error downloading data for range {from_date} to {min_date_in_file - timedelta(days=1)}: {e}")

            # For the end date, prefer DB's max(date) if available
            effective_max_existing = db_max_date if db_max_date is not None else max_date_in_file
            if effective_max_existing is not None and to_date > effective_max_existing:
                self._log(f"Fetching data from {effective_max_existing + timedelta(days=1)} to {to_date}")
                try:
                    after_df = fetch_with_primary_and_fallback(
                        f=effective_max_existing + timedelta(days=1),
                        t=to_date,
                    )
                    new_data_list.append(after_df)
                except Exception as e:
                    self._log(f"Error downloading data for range {effective_max_existing + timedelta(days=1)} to {to_date}: {e}")

        else:
            # No CSV present or it's empty; if DB has an end date, continue from there
            if db_max_date is not None:
                self._log(f"No existing CSV or CSV empty. Using DB end date {db_max_date} to continue.")
                try:
                    from_after = db_max_date + timedelta(days=1)
                    if from_after <= to_date:
                        after_df = fetch_with_primary_and_fallback(
                            f=from_after,
                            t=to_date,
                        )
                        new_data_list.append(after_df)
                    else:
                        self._log("No new dates to fetch after DB end date.")
                except Exception as e:
                    self._log(f"Error downloading data for range {db_max_date + timedelta(days=1)} to {to_date}: {e}")
            else:
                self._log("No existing data found in CSV or DB. Downloading full date range.")
                try:
                    full_df = fetch_with_primary_and_fallback(
                        f=from_date,
                        t=to_date,
                    )
                    new_data_list.append(full_df)
                except Exception as e:
                    self._log(f"Error downloading data for range {from_date} to {to_date}: {e}")

        if not new_data_list and existing_df is not None:
            self._log("No new data to download. File is already up-to-date for the given range.")
            return existing_df

        all_data = [existing_df] if existing_df is not None else []
        all_data.extend(new_data_list)

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['DATE'] = pd.to_datetime(combined_df['DATE']).dt.date
        combined_df = combined_df.drop_duplicates(subset=['DATE'], keep='last')
        combined_df = combined_df.sort_values(by='DATE', ascending=False)

        self._log(f"\nData shape: {combined_df.shape}")
        self._log(f"Columns: {list(combined_df.columns)}")
        self._log("\nFirst 5 rows:")
        self._log(str(combined_df.head()))

        self._log("\nLast 5 rows:")
        self._log(str(combined_df.tail()))

        combined_df.to_csv(filename, index=False)
        self._log(f"\nData saved to: {filename}")

        self._log("\nBasic statistics:")
        self._log(f"Date range: {combined_df['DATE'].min()} to {combined_df['DATE'].max()}")
        if 'VOLUME' in combined_df.columns:
            self._log(f"Number of trading days: {len(combined_df)}")
            self._log(f"Average volume: {combined_df['VOLUME'].mean():,.0f}")
        if 'CLOSE' in combined_df.columns:
            self._log(f"Average close price: ₹{combined_df['CLOSE'].mean():.2f}")

        return combined_df

    def download_index_data(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        filename: str | None = None,
    ) -> None:
        """
        Download historical index data for a given symbol and date range using nselib.
        If a filename is provided and the file exists, it will only download the missing data.
        """
        if filename is None:
            filename = f"{symbol}_data_{from_date}_{to_date}.csv"

        filename = filename.replace(" ", "_")

        self._log(f"Downloading historical data for {symbol} from {from_date} to {to_date}")

        from_date_str = from_date.strftime('%d-%m-%Y')
        to_date_str = to_date.strftime('%d-%m-%Y')

        try:
            self._log(f"nselib index_data {symbol}: {from_date_str} → {to_date_str}")
            with self._timeout(self.timeout_seconds, f"nselib index timeout for {symbol} {from_date_str}→{to_date_str}"):
                df = self._get_capital_market().index_data(symbol, from_date=from_date_str, to_date=to_date_str)
            if df.empty:
                raise FileNotFoundError(f"No data available for {symbol} in the given date range.")

            df = df.rename(columns={'TIMESTAMP': 'Date', 'CLOSE_INDEX_VAL': 'Close'})

            df.to_csv(filename, index=False)
            self._log(f"\nData saved to: {filename}")

            self._log("\nBasic statistics:")
            df['Date'] = pd.to_datetime(df['Date'])
            self._log(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
            self._log(f"Number of trading days: {len(df)}")
            self._log(f"Average close price: ₹{df['Close'].mean():.2f}")

        except Exception as e:
            self._log(f"Error downloading data for {symbol}: {e}")
            raise 