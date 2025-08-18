import streamlit as st
import pandas as pd
import altair as alt
import duckdb
import os

st.set_page_config(layout="wide")
st.title('Stock Dash')

# --- Database Connection ---
@st.cache_resource
def get_db_connection():
    """
    Establishes a connection to the DuckDB database.
    The connection is cached to avoid reconnecting on every interaction.
    """
    return duckdb.connect('stock_data.db', read_only=True)

con = get_db_connection()

# --- Data Loading Functions ---
@st.cache_data
def get_unified_symbols():
    """
    Gets a unified list of all symbols (stocks and indices) and a mapping to their type.
    """
    stocks_df = con.execute("SELECT DISTINCT symbol FROM stock_prices").df()
    stocks = stocks_df['symbol'].tolist()
    
    indices_df = con.execute("SELECT DISTINCT symbol FROM index_prices").df()
    indices = indices_df['symbol'].tolist()
    
    symbol_map = {symbol: 'Stock' for symbol in stocks}
    symbol_map.update({symbol: 'Index' for symbol in indices})
    
    all_symbols = sorted(stocks + indices)
    return all_symbols, symbol_map

def load_data(symbol, data_type):
    """
    Loads data for a given symbol from the correct database table.
    """
    table = 'index_prices' if data_type == 'Index' else 'stock_prices'
    query = f"SELECT * FROM {table} WHERE symbol = ?"
    df = con.execute(query, [symbol]).df()
    return df

def create_ohlc_chart(df):
    """
    Creates an OHLC chart using Altair from the database data.
    """
    base = alt.Chart(df).encode(
        alt.X('date:T', axis=alt.Axis(title='Date')),
        color=alt.condition("datum.open < datum.close", alt.value("#06982d"), alt.value("#ae1325"))
    )

    chart = alt.layer(
        base.mark_rule().encode(
            alt.Y('low:Q', title='Price', scale=alt.Scale(zero=False)),
            alt.Y2('high:Q')
        ),
        base.mark_bar().encode(
            alt.Y('open:Q'),
            alt.Y2('close:Q')
        )
    ).interactive()

    return chart

# --- App Layout ---
all_symbols, symbol_map = get_unified_symbols()

# Use a single selectbox for search and selection
options = ["Select or search for a symbol"] + all_symbols
selected_symbol = st.selectbox('Search and select a symbol', options)

if selected_symbol and selected_symbol != options[0]:
    data_type = symbol_map[selected_symbol]
    data = load_data(selected_symbol, data_type)
    
    if not data.empty:
        st.subheader(f'Displaying data for {selected_symbol} ({data_type})')
        chart = create_ohlc_chart(data)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.error(f"Could not load data for {selected_symbol}") 