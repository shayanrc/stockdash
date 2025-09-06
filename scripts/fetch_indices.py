import argparse
import os
from datetime import date
from lib.clients.nse_client import download_index_data

CACHE_INDEX_DIR = 'data/cache/index_history'
DEFAULT_INDICES = [
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

def main(indices, start: date, end: date, delay: int):
	os.makedirs(CACHE_INDEX_DIR, exist_ok=True)
	for i, index_name in enumerate(indices, 1):
		print(f"\n{'='*60}\nProcessing {i}/{len(indices)}: {index_name}\n{'='*60}")
		filename = index_name.replace(' ', '_') + '.csv'
		filepath = os.path.join(CACHE_INDEX_DIR, filename)
		try:
			download_index_data(symbol=index_name, from_date=start, to_date=end, filename=filepath)
		except Exception as e:
			print(f"Failed {index_name}: {e}")
		if i < len(indices) and delay > 0:
			import time
			time.sleep(delay)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Fetch index history to cache directory.')
	parser.add_argument('--start', default='2020-01-01', help='Start date YYYY-MM-DD')
	parser.add_argument('--end', default=date.today().strftime('%Y-%m-%d'), help='End date YYYY-MM-DD')
	parser.add_argument('--delay', type=int, default=2, help='Delay between requests (seconds)')
	parser.add_argument('--indices', nargs='*', default=DEFAULT_INDICES, help='List of indices to download')
	args = parser.parse_args()
	start_date = date.fromisoformat(args.start)
	end_date = date.fromisoformat(args.end)
	main(indices=args.indices, start=start_date, end=end_date, delay=args.delay) 