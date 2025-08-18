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

@st.cache_data
def load_data(symbol, data_type):
    """
    Loads data for a given symbol from the correct database table.
    """
    table = 'index_prices' if data_type == 'Index' else 'stock_prices'
    query = f"SELECT * FROM {table} WHERE symbol = ? ORDER BY date ASC"
    df = con.execute(query, [symbol]).df()
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache_data
def create_ohlc_chart(df, rolling_window=None):
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

    if rolling_window:
        overlay_df = df.dropna(subset=['rolling_mean', 'upper_bound', 'lower_bound',
                                     'upper_bound_2sigma', 'lower_bound_2sigma',
                                     'upper_bound_3sigma', 'lower_bound_3sigma'])

        rolling_avg_line = alt.Chart(overlay_df).mark_line(
            color='white',
            strokeWidth=2
        ).encode(
            x='date:T',
            y='rolling_mean:Q'
        )

        upper_bound_line = alt.Chart(overlay_df).mark_line(
            color='#ffb366', # 1-sigma upper bound
            opacity=0.3
        ).encode(
            x='date:T',
            y='upper_bound:Q'
        )

        lower_bound_line = alt.Chart(overlay_df).mark_line(
            color='#99ff99', # 1-sigma lower bound
            opacity=0.3
        ).encode(
            x='date:T',
            y='lower_bound:Q'
        )

        upper_bound_2sigma_line = alt.Chart(overlay_df).mark_line(
            color='#ff6600', # 2-sigma upper bound
            opacity=0.6
        ).encode(
            x='date:T',
            y='upper_bound_2sigma:Q'
        )

        lower_bound_2sigma_line = alt.Chart(overlay_df).mark_line(
            color='#33cc33', # 2-sigma lower bound
            opacity=0.6
        ).encode(
            x='date:T',
            y='lower_bound_2sigma:Q'
        )

        upper_bound_3sigma_line = alt.Chart(overlay_df).mark_line(
            color='#cc0000', # 3-sigma upper bound
            opacity=0.9
        ).encode(
            x='date:T',
            y='upper_bound_3sigma:Q'
        )

        lower_bound_3sigma_line = alt.Chart(overlay_df).mark_line(
            color='#009900', # 3-sigma lower bound
            opacity=0.9
        ).encode(
            x='date:T',
            y='lower_bound_3sigma:Q'
        )

        chart = alt.layer(chart, rolling_avg_line, upper_bound_line, lower_bound_line,
                          upper_bound_2sigma_line, lower_bound_2sigma_line,
                          upper_bound_3sigma_line, lower_bound_3sigma_line).resolve_scale(y='shared')

    return chart


# --- App Layout ---
all_symbols, symbol_map = get_unified_symbols()

# Use a single selectbox for search and selection
options = ["Select or search for a symbol"] + all_symbols
selected_symbol = st.selectbox('Search and select a symbol', options)

if selected_symbol and selected_symbol != options[0]:
    data_type = symbol_map[selected_symbol]
    data = load_data(selected_symbol, data_type)
    
    # Add a slider for the rolling average
    rolling_window = st.slider('simple moving average window (days)', min_value=1, max_value=500, value=5, step=1)
    
    if not data.empty:
        if rolling_window:
            data['rolling_mean'] = data['close'].rolling(window=rolling_window).mean()
            data['rolling_std'] = data['close'].rolling(window=rolling_window).std()
            data['upper_bound'] = data['rolling_mean'] + data['rolling_std']
            data['lower_bound'] = data['rolling_mean'] - data['rolling_std']
            data['upper_bound_2sigma'] = data['rolling_mean'] + 2 * data['rolling_std']
            data['lower_bound_2sigma'] = data['rolling_mean'] - 2 * data['rolling_std']
            data['upper_bound_3sigma'] = data['rolling_mean'] + 3 * data['rolling_std']
            data['lower_bound_3sigma'] = data['rolling_mean'] - 3 * data['rolling_std']

        st.subheader(f'Displaying data for {selected_symbol} ({data_type})')
        chart = create_ohlc_chart(data, rolling_window=rolling_window)
        st.altair_chart(chart, use_container_width=True)
        
        st.subheader('Recent Data')
        st.dataframe(data.tail())
    else:
        st.error(f"Could not load data for {selected_symbol}") 