import duckdb
import os
import pandas as pd
import re

def clean_col_names(df):
    """
    Cleans column names of a pandas DataFrame.
    """
    cols = df.columns
    new_cols = []
    for col in cols:
        new_col = re.sub(r'[^A-Za-z0-9_]+', '', col)
        new_cols.append(new_col)
    df.columns = new_cols
    return df

def get_latest_date(con, table, symbol_col, symbol):
    """
    Gets the latest date for a given symbol from a specified table.
    """
    result = con.execute(f"SELECT MAX(date) FROM {table} WHERE {symbol_col} = ?", [symbol]).fetchone()
    return result[0] if result and result[0] else None

def load_index_data(con):
    """
    Loads data from the index_history CSV files into the index_prices table.
    """
    folder_path = 'data/index_history'
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            index_symbol = filename.replace('.csv', '')
            
            latest_date = get_latest_date(con, 'index_prices', 'symbol', index_symbol)
            
            df = pd.read_csv(filepath)
            df.drop_duplicates(inplace=True)
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')

            if latest_date:
                df = df[df['Date'] > pd.to_datetime(latest_date)]

            if df.empty:
                print(f"No new data for index: {index_symbol}")
                continue

            df = clean_col_names(df)
            
            con.register('temp_index_df', df)
            
            insert_query = """
            INSERT OR IGNORE INTO index_prices
            SELECT
                Date,
                INDEX_NAME,
                OPEN_INDEX_VAL,
                HIGH_INDEX_VAL,
                LOW_INDEX_VAL,
                Close,
                TRADED_QTY,
                TURN_OVER
            FROM temp_index_df
            """

            con.execute(insert_query)
            con.unregister('temp_index_df')
            print(f"Loaded new data for index: {index_symbol}")

def load_stock_data(con):
    """
    Loads data from the price_history CSV files into the stock_prices table.
    """
    folder_path = 'data/price_history'
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            stock_symbol = filename.replace('.csv', '')
            
            latest_date = get_latest_date(con, 'stock_prices', 'symbol', stock_symbol)
            
            df = pd.read_csv(filepath)
            df.drop_duplicates(inplace=True)
            df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m-%d')

            if latest_date:
                df = df[df['DATE'] > pd.to_datetime(latest_date)]

            if df.empty:
                print(f"No new data for stock: {stock_symbol}")
                continue

            df = clean_col_names(df)

            con.register('temp_stock_df', df)

            insert_query = """
            INSERT OR IGNORE INTO stock_prices
            SELECT
                DATE,
                SYMBOL,
                'NSE' AS exchange,
                OPEN,
                HIGH,
                LOW,
                PREVCLOSE,
                LTP,
                CLOSE,
                VWAP,
                VOLUME,
                VALUE,
                NOOFTRADES
            FROM temp_stock_df
            """
            
            con.execute(insert_query)
            con.unregister('temp_stock_df')
            print(f"Loaded new data for stock: {stock_symbol}")

def main(db_file='stock_data.db'):
    """
    Main function to connect to the database and load data.
    """
    con = duckdb.connect(database=db_file, read_only=False)
    
    load_index_data(con)
    load_stock_data(con)
    
    con.close()
    print(f"Database '{db_file}' has been successfully updated.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Load CSV histories into DuckDB tables.')
    parser.add_argument('--db-file', default='stock_data.db', help='Path to DuckDB database file')
    args = parser.parse_args()
    main(db_file=args.db_file) 