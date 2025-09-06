import os
from datetime import date, timedelta
import pandas as pd
from nselib import capital_market


def _fetch_equity_history_nselib(symbol: str, from_date: date, to_date: date, series: str = "EQ") -> pd.DataFrame:
	if from_date > to_date:
		return pd.DataFrame(columns=[
			"DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL", "SERIES"
		])

	chunk_days = 60
	all_chunks: list[pd.DataFrame] = []
	start = from_date
	while start <= to_date:
		end = min(start + timedelta(days=chunk_days - 1), to_date)
		# Some NSE endpoints reject ranges where from == to; widen call window by +1 day when needed
		end_call = end if end > start else start + timedelta(days=1)
		start_str = start.strftime('%d-%m-%Y')
		end_str = end_call.strftime('%d-%m-%Y')
		try:
			raw_df = capital_market.price_volume_data(symbol, start_str, end_str)
			if raw_df is not None and not raw_df.empty:
				df = raw_df.copy()
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
				normalized["SERIES"] = df.get('Series', series)
				normalized = normalized.dropna(subset=["DATE"])  # type: ignore[arg-type]
				all_chunks.append(normalized)
		except Exception as e:
			print(f"Error fetching equity history for {symbol} {start_str} to {end_str}: {e}")
		finally:
			# Advance based on the intended chunk end (not widened end_call)
			start = end + timedelta(days=1)

	if not all_chunks:
		return pd.DataFrame(columns=[
			"DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL", "SERIES"
		])

	out = pd.concat(all_chunks, ignore_index=True)
	out["DATE"] = pd.to_datetime(out["DATE"], errors='coerce').dt.date
	# Clamp to the requested inclusive window in case widened calls fetched extra rows
	out = out[(out["DATE"] >= from_date) & (out["DATE"] <= to_date)]
	out = out.dropna(subset=["DATE"])  # type: ignore[arg-type]
	out = out.drop_duplicates(subset=["DATE"], keep='last')
	out = out.sort_values(by="DATE").reset_index(drop=True)
	return out


def download_stock_data(symbol, from_date, to_date, filename=None):
	if filename is None:
		filename = f"{symbol}_data_{from_date}_{to_date}.csv"

	existing_df = None
	if filename and os.path.exists(filename):
		existing_df = pd.read_csv(filename)
		existing_df['DATE'] = pd.to_datetime(existing_df['DATE']).dt.date

	new_data_list = []

	if existing_df is not None and not existing_df.empty:
		min_date_in_file = existing_df['DATE'].min()
		max_date_in_file = existing_df['DATE'].max()

		if from_date < min_date_in_file:
			before_df = _fetch_equity_history_nselib(
				symbol=symbol,
				from_date=from_date,
				to_date=min_date_in_file - timedelta(days=1),
				series="EQ",
			)
			new_data_list.append(before_df)

		if to_date > max_date_in_file:
			after_df = _fetch_equity_history_nselib(
				symbol=symbol,
				from_date=max_date_in_file + timedelta(days=1),
				to_date=to_date,
				series="EQ",
			)
			new_data_list.append(after_df)
	else:
		full_df = _fetch_equity_history_nselib(
			symbol=symbol,
			from_date=from_date,
			to_date=to_date,
			series="EQ",
		)
		new_data_list.append(full_df)

	# Build list of non-empty frames only, to avoid pandas concat warnings and dtype ambiguity
	frames: list[pd.DataFrame] = []
	if existing_df is not None and not existing_df.empty:
		frames.append(existing_df)
	for df in new_data_list:
		if df is not None and not df.empty:
			frames.append(df)

	if not frames:
		# Ensure an on-disk file with the correct schema even if no data was available
		empty = pd.DataFrame(columns=["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL", "SERIES"])
		empty.to_csv(filename, index=False)
		return empty

	combined_df = pd.concat(frames, ignore_index=True)
	combined_df['DATE'] = pd.to_datetime(combined_df['DATE']).dt.date
	combined_df = combined_df.drop_duplicates(subset=['DATE'], keep='last')
	combined_df = combined_df.sort_values(by='DATE', ascending=False)
	combined_df.to_csv(filename, index=False)
	return combined_df


def download_index_data(symbol, from_date, to_date, filename=None):
	if filename is None:
		filename = f"{symbol}_data_{from_date}_{to_date}.csv"
	filename = filename.replace(" ", "_")
	from_date_str = from_date.strftime('%d-%m-%Y')
	to_date_str = to_date.strftime('%d-%m-%Y')
	df = capital_market.index_data(symbol, from_date=from_date_str, to_date=to_date_str)
	if df.empty:
		raise FileNotFoundError(f"No data available for {symbol} in the given date range.")
	df = df.rename(columns={'TIMESTAMP': 'Date', 'CLOSE_INDEX_VAL': 'Close'})
	df.to_csv(filename, index=False)
	return df 