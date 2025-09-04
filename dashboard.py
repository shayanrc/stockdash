import streamlit as st
import pandas as pd
import altair as alt
import duckdb
import os

st.set_page_config(layout="wide", page_title="Stock Dash")
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


# --- Callback Functions ---
def set_rolling_window(value):
    st.session_state.rolling_window = value

# --- App Layout ---
all_symbols, symbol_map = get_unified_symbols()

# Use a single selectbox for search and selection
options = ["Select or search for a symbol"] + all_symbols
selected_symbol = st.selectbox('Search and select a symbol', options)

if selected_symbol and selected_symbol != options[0]:
    data_type = symbol_map[selected_symbol]
    data = load_data(selected_symbol, data_type)
    
    # Get widget values from session state, providing defaults for the first run
    if 'rolling_window' not in st.session_state:
        st.session_state['rolling_window'] = 5
    if 'use_vwap' not in st.session_state:
        st.session_state['use_vwap'] = False

    rolling_window = st.session_state['rolling_window']
    use_vwap = st.session_state['use_vwap']

    # Determine which price type to use for calculations
    column_to_use = 'close'
    if data_type == 'Stock' and use_vwap:
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

        st.subheader(f'Displaying data for {selected_symbol} ({data_type})')
        
        # --- Metrics Display ---
        col1, col2, col3, col4 = st.columns(4)
        col_lu, col5, col6, col7 = st.columns(4)

        # Always display Price and Last Updated if data is available
        if not data.empty:
            price_delta_str = None
            if len(data) >= 2:
                price = data['close'].iloc[-1]
                prev_price = data['close'].iloc[-2]
                price_delta = ((price - prev_price) / prev_price) * 100
                price_delta_str = f"{price_delta:.2f}%"
            else:
                price = data['close'].iloc[-1]
                
            col1.metric("Price", f"{price:,.2f}", delta=price_delta_str)
            latest_date = data['date'].max().strftime('%b %d, %Y')
            col_lu.metric("Last Updated", latest_date)

        # Display sigma metrics only if a rolling window is active
        if rolling_window and 'upper_bound' in data.columns:
            required_cols = [
                'close', 'upper_bound', 'lower_bound', 'upper_bound_2sigma', 
                'lower_bound_2sigma', 'upper_bound_3sigma', 'lower_bound_3sigma'
            ]
            latest_metrics_df = data.dropna(subset=required_cols).tail(2)
            
            if not latest_metrics_df.empty:
                latest_metrics = latest_metrics_df.iloc[-1]
                upper_bound = latest_metrics['upper_bound']
                lower_bound = latest_metrics['lower_bound']
                upper_bound_2s = latest_metrics['upper_bound_2sigma']
                lower_bound_2s = latest_metrics['lower_bound_2sigma']
                upper_bound_3s = latest_metrics['upper_bound_3sigma']
                lower_bound_3s = latest_metrics['lower_bound_3sigma']

                delta_values = {}
                if len(latest_metrics_df) == 2:
                    previous_metrics = latest_metrics_df.iloc[0]
                    delta_values['upper_bound'] = f"{upper_bound - previous_metrics['upper_bound']:.2f}"
                    delta_values['lower_bound'] = f"{lower_bound - previous_metrics['lower_bound']:.2f}"
                    delta_values['upper_bound_2s'] = f"{upper_bound_2s - previous_metrics['upper_bound_2sigma']:.2f}"
                    delta_values['lower_bound_2s'] = f"{lower_bound_2s - previous_metrics['lower_bound_2sigma']:.2f}"
                    delta_values['upper_bound_3s'] = f"{upper_bound_3s - previous_metrics['upper_bound_3sigma']:.2f}"
                    delta_values['lower_bound_3s'] = f"{lower_bound_3s - previous_metrics['lower_bound_3sigma']:.2f}"

                col2.metric("1 Sigma Upper", f"{upper_bound:,.2f}", delta=delta_values.get('upper_bound'))
                col3.metric("2 Sigma Upper", f"{upper_bound_2s:,.2f}", delta=delta_values.get('upper_bound_2s'))
                col4.metric("3 Sigma Upper", f"{upper_bound_3s:,.2f}", delta=delta_values.get('upper_bound_3s'))
                col5.metric("1 Sigma Lower", f"{lower_bound:,.2f}", delta=delta_values.get('lower_bound'))
                col6.metric("2 Sigma Lower", f"{lower_bound_2s:,.2f}", delta=delta_values.get('lower_bound_2s'))
                col7.metric("3 Sigma Lower", f"{lower_bound_3s:,.2f}", delta=delta_values.get('lower_bound_3s'))

        chart = create_ohlc_chart(data, rolling_window=rolling_window)
        st.altair_chart(chart, use_container_width=True)
        
        with st.container():
            st.slider(
                'Simple Moving Average Window (days)', 
                min_value=0, 
                max_value=500, 
                step=5, 
                key='rolling_window'
            )

            # --- Control Row for VWAP toggle and SMA presets ---
            if data_type == 'Stock':
                col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])
                with col1:
                    st.toggle("Volume Weighted", key='use_vwap')
                
                button_cols = [col2, col3, col4, col5]
                button_values = [20, 50, 100, 200]
                for i, val in enumerate(button_values):
                    button_cols[i].button(f'{val} days', use_container_width=True, on_click=set_rolling_window, args=(val,))

            else:
                st.write("") # Placeholder to create space
                col1, col2, col3, col4 = st.columns(4)
                button_cols = [col1, col2, col3, col4]
                button_values = [20, 50, 100, 200]

                for i, val in enumerate(button_values):
                    button_cols[i].button(f'{val} days', use_container_width=True, on_click=set_rolling_window, args=(val,))

        # st.subheader('Recent Data')
        # st.dataframe(data.tail())
    else:
        st.error(f"Could not load data for {selected_symbol}") 