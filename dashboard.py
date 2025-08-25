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
    Creates an OHLC chart with Bollinger-style bands and a volume chart.
    """
    # Define the base chart with common encodings
    base = alt.Chart(df).encode(
        alt.X('date:T', axis=alt.Axis(title=None, labels=False))
    )

    # OHLC chart layer
    ohlc = alt.layer(
        base.mark_rule().encode(
            alt.Y('low:Q', title='Price', scale=alt.Scale(zero=False)),
            alt.Y2('high:Q')
        ),
        base.mark_bar().encode(
            alt.Y('open:Q'),
            alt.Y2('close:Q')
        )
    ).encode(
        color=alt.condition("datum.open < datum.close", alt.value("#06982d"), alt.value("#ae1325"))
    )

    price_chart = ohlc

    if rolling_window:
        overlay_df = df.dropna(subset=['rolling_mean', 'upper_bound', 'lower_bound',
                                     'upper_bound_2sigma', 'lower_bound_2sigma',
                                     'upper_bound_3sigma', 'lower_bound_3sigma'])

        # --- Define Area Bands (Fill only) ---
        ub3_area = alt.Chart(overlay_df).mark_area(opacity=0.2, color='#FF4500').encode(y='upper_bound_3sigma:Q', y2='upper_bound_2sigma:Q', x='date:T')
        ub2_area = alt.Chart(overlay_df).mark_area(opacity=0.15, color='#FF7F50').encode(y='upper_bound_2sigma:Q', y2='upper_bound:Q', x='date:T')
        ub1_area = alt.Chart(overlay_df).mark_area(opacity=0.1, color='#FFBF00').encode(y='upper_bound:Q', y2='rolling_mean:Q', x='date:T')

        lb1_area = alt.Chart(overlay_df).mark_area(opacity=0.1, color='#AFEEEE').encode(y='rolling_mean:Q', y2='lower_bound:Q', x='date:T')
        lb2_area = alt.Chart(overlay_df).mark_area(opacity=0.15, color='#40E0D0').encode(y='lower_bound:Q', y2='lower_bound_2sigma:Q', x='date:T')
        lb3_area = alt.Chart(overlay_df).mark_area(opacity=0.2, color='#008B8B').encode(y='lower_bound_2sigma:Q', y2='lower_bound_3sigma:Q', x='date:T')

        # --- Define Bound Lines ---
        line_opacity = 0.5
        ub3_line = alt.Chart(overlay_df).mark_line(color='#FF4500', opacity=line_opacity).encode(x='date:T', y='upper_bound_3sigma:Q')
        ub2_line = alt.Chart(overlay_df).mark_line(color='#FF7F50', opacity=line_opacity).encode(x='date:T', y='upper_bound_2sigma:Q')
        ub1_line = alt.Chart(overlay_df).mark_line(color='#FFBF00', opacity=line_opacity).encode(x='date:T', y='upper_bound:Q')
        lb1_line = alt.Chart(overlay_df).mark_line(color='#AFEEEE', opacity=line_opacity).encode(x='date:T', y='lower_bound:Q')
        lb2_line = alt.Chart(overlay_df).mark_line(color='#40E0D0', opacity=line_opacity).encode(x='date:T', y='lower_bound_2sigma:Q')
        lb3_line = alt.Chart(overlay_df).mark_line(color='#008B8B', opacity=line_opacity).encode(x='date:T', y='lower_bound_3sigma:Q')

        # Rolling average line
        rolling_avg_line = alt.Chart(overlay_df).mark_line(color='white', strokeWidth=2, opacity=0.5).encode(x='date:T', y='rolling_mean:Q')

        price_chart = alt.layer(
            ub3_area, ub2_area, ub1_area, lb1_area, lb2_area, lb3_area,
            ub3_line, ub2_line, ub1_line, lb1_line, lb2_line, lb3_line,
            ohlc, 
            rolling_avg_line
        )

    # Volume Chart
    volume_chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('date:T', axis=alt.Axis(title='Date')),
        y=alt.Y('volume:Q', axis=alt.Axis(title='Volume')),
        color=alt.condition("datum.open < datum.close", alt.value("#06982d"), alt.value("#ae1325"))
    ).properties(height=100)

    # Combine charts vertically
    final_chart = alt.vconcat(
        price_chart.properties(height=450).interactive(),
        volume_chart
    ).resolve_scale(x='shared')

    return final_chart


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
    
    # Determine which price type to use for calculations
    column_to_use = 'close'
    if data_type == 'Stock':
        use_vwap = st.toggle("Volume Weighted")
        if use_vwap:
            column_to_use = 'vwap'
    
    if not data.empty:
        if rolling_window:
            data['rolling_mean'] = data[column_to_use].rolling(window=rolling_window).mean()
            data['rolling_std'] = data[column_to_use].rolling(window=rolling_window).std()
            data['upper_bound'] = data['rolling_mean'] + data['rolling_std']
            data['lower_bound'] = data['rolling_mean'] - data['rolling_std']
            data['upper_bound_2sigma'] = data['rolling_mean'] + 2 * data['rolling_std']
            data['lower_bound_2sigma'] = data['rolling_mean'] - 2 * data['rolling_std']
            data['upper_bound_3sigma'] = data['rolling_mean'] + 3 * data['rolling_std']
            data['lower_bound_3sigma'] = data['rolling_mean'] - 3 * data['rolling_std']
        last_close = data['close'].iloc[-1]
        prev_close = data['close'].iloc[-2]
        delta = 100*(last_close - prev_close)/prev_close

        st.metric(f'{selected_symbol} ({data_type})', value=last_close, delta=f"{delta:.2f}%")
        chart = create_ohlc_chart(data, rolling_window=rolling_window)
        st.altair_chart(chart, use_container_width=True)
        
    else:
        st.error(f"Could not load data for {selected_symbol}") 