import streamlit as st
import pandas as pd
import altair as alt
import duckdb
import os

st.title('Stock and Index OHLC Chart from DuckDB')

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
def get_symbols(table_name):
    """
    Gets a list of symbols from the specified database table.
    """
    query = f"SELECT DISTINCT symbol FROM {table_name}"
    symbols = con.execute(query).df()['symbol'].tolist()
    return sorted(symbols)

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
data_type = st.radio("Select data type", ('Stock', 'Index'))

if data_type == 'Stock':
    symbols = get_symbols('stock_prices')
else:
    symbols = get_symbols('index_prices')

selected_symbol = st.selectbox('Select a symbol', symbols)

if selected_symbol:
    data = load_data(selected_symbol, data_type)
    
    if not data.empty:
        st.subheader(f'Displaying data for {selected_symbol}')
        chart = create_ohlc_chart(data)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.error(f"Could not load data for {selected_symbol}") 