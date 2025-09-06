## Architecture

### Overview
This document describes the end-to-end data flow and components of the Stock Dash application. The system ingests market data from NSE via `nselib`, stages it into CSV files, incrementally loads it into a DuckDB database, and visualizes it in a Streamlit dashboard.

### Data Flow Diagram
```mermaid
graph LR

  subgraph Sources
    A["NSE APIs via nselib<br/>(capital_market.price_volume_data,<br/>capital_market.index_data)"]
    C0["Universe CSV<br/>data/universe/nse_nifty500.csv"]
  end

  subgraph Ingestion
    direction TB
    B2["scripts/fetch_indices.py<br/>download_index_data(index, from, to)"]
    B1["scripts/fetch_stocks.py<br/>download_stock_data(symbol, from, to)"]
    H["Symbols (from universe_stocks)"]
  end

  subgraph Staging
    direction TB
    C2["CSV files<br/>data/cache/index_history/*.csv"]
    C1["CSV files<br/>data/cache/price_history/*.csv"]
  end

  subgraph ETL / Storage
    direction LR
    D0["init_local_db.py<br/>(create schemas)"]
    D1["populate_local_db.py<br/>(load universe_stocks from CSV)"]
    D2["load_to_duckdb.py<br/>(incremental load: stocks + indices)"]
    E["DuckDB: data/db/stock.duckdb<br/>Tables: stock_prices, index_prices, universe_stocks"]
  end

  subgraph App
    F["dashboard.py (Streamlit)"]
    G["Altair charts + metrics"]
  end

  %% Flows
  A -->|indices| B2
  A -->|stocks| B1
  B2 -->|write| C2
  B1 -->|write| C1
  C2 -->|read| D2
  C1 -->|read| D2
  D2 -->|INSERT OR IGNORE| E

  C0 -->|read| D1
  D0 --> E
  D1 --> E

  %% Universe symbols exposed near ingestion to avoid crossing blocks
  E --> H
  H --> B1

  F -->|read-only connect| E
  F --> G
```

### Components
- **Data sources**: NSE endpoints accessed through `nselib.capital_market`.
- **Ingestion scripts**:
  - `scripts/fetch_stocks.py`: Reads stock symbols from DuckDB `universe_stocks` (once populated), downloads per-symbol history to `data/cache/price_history/<SYMBOL>.csv`. CLI: `--db-file`, `--exchange`, `--start`, `--end` (default today), `--delay`, `--limit`.
  - `scripts/fetch_indices.py`: Iterates a set of indices and downloads history to `data/cache/index_history/<INDEX>.csv`. CLI: `--indices`, `--start`, `--end` (default today), `--delay`.
- **ETL / Storage**:
  - `init_local_db.py`: Creates DuckDB schemas for `universe_stocks`, `index_prices`, and `stock_prices`. CLI: `--db-file`.
  - `populate_local_db.py`: Populates `universe_stocks` from `data/universe/nse_nifty500.csv`. CLI: `--db-file`, `--csv-file`.
  - `load_to_duckdb.py`: Incrementally loads new rows from cache CSVs into DuckDB. CLI: `--db-file`.
- **Application**:
  - `dashboard.py`: Streamlit UI; queries DuckDB read-only, computes rolling metrics, and renders OHLC + volume charts with Altair.

### Ingestion details
- **Stocks (`download_stock_data`)**
  - If a CSV exists, determines existing min/max `DATE` and fetches only missing ranges.
  - Fetches in safe 60-day chunks via `_fetch_equity_history_nselib(symbol, from, to)`; widens same-day API requests (+1 day) and clamps results back to the requested range.
  - Normalizes columns to: `DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL, SERIES` (CSV).
  - De-duplicates by `DATE` and sorts before writing.
- **Indices (`download_index_data`)**
  - Calls `capital_market.index_data` for `[from_date, to_date]` and writes CSV after renaming `TIMESTAMP→Date`, `CLOSE_INDEX_VAL→Close`.

### Database schemas (`init_local_db.py`)
- Creates the following tables if not present:
```sql
CREATE TABLE IF NOT EXISTS universe_stocks (
  "Company Name" VARCHAR NOT NULL,
  "Industry"     VARCHAR,
  "Symbol"       VARCHAR NOT NULL,
  "Exchange"     VARCHAR DEFAULT 'NSE',
  "code"         VARCHAR,
  PRIMARY KEY ("Symbol", "Exchange")
);

CREATE TABLE IF NOT EXISTS index_prices (
  date DATE,
  symbol VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  turnover DOUBLE,
  PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS stock_prices (
  date DATE,
  symbol VARCHAR,
  exchange VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  prev_close DOUBLE,
  ltp DOUBLE,
  close DOUBLE,
  vwap DOUBLE,
  volume BIGINT,
  value DOUBLE,
  trades BIGINT,
  PRIMARY KEY (date, symbol, exchange)
);
```

### ETL details
- **Populate universe (`populate_local_db.py`)**
  - Loads from `data/universe/nse_nifty500.csv` with mapping: `Series`→`Exchange='NSE'`, `ISIN Code`→`code`.
- **Load histories (`load_to_duckdb.py`)**
  - Index data: inserts into `index_prices` selecting expected fields.
  - Stock data: inserts into `stock_prices` with `exchange` set to `'NSE'`:
```sql
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
FROM temp_stock_df;
```

### Dashboard and analytics (`dashboard.py`)
- Maintains a cached read-only DuckDB connection (`st.cache_resource`).
- Builds a unified symbol list from `stock_prices` and `index_prices`.
- On selection, loads `WHERE symbol = ? ORDER BY date ASC` from the appropriate table.
- Computes rolling mean/std and ±1/2/3 sigma bands on `close` (or `vwap` when toggled for stocks) and renders:
  - OHLC with colored bars and wicks.
  - Volume sub-chart.
  - KPI metrics for current Price, Last Updated, and sigma bounds (with deltas when possible).

### Data formats
- **Stock CSV** (per symbol): `DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL, SERIES`.
- **Index CSV** (per index): includes `Date`, `Close`, and other NSE-provided columns; file name uses underscores for spaces.
- **Database tables**:
  - `universe_stocks`: company metadata and exchange/code mapping.
  - `stock_prices`: OHLCV and related fields with `exchange` key.
  - `index_prices`: OHLCV and turnover for indices.

### Incrementality and idempotency
- Ingestion fetches only missing date ranges relative to existing CSVs.
- ETL inserts only rows with `date > MAX(date)` per symbol; `INSERT OR IGNORE` guards PK collisions.

### Operations
- **Initialize schemas**: `python scripts/init_localdb.py` (or `python init_local_db.py --db-file data/db/stock.duckdb`)
- **Populate universe**: `python scripts/populate_universe.py` (or `python populate_local_db.py --db-file data/db/stock.duckdb --csv-file data/universe/nse_nifty500.csv`)
- **Update indices CSVs**: `python -m scripts.fetch_indices --start 2020-01-01`
- **Update stocks CSVs**: `python -m scripts.fetch_stocks --db-file data/db/stock.duckdb --start 2020-01-01 --delay 2`
- **Load/refresh DB**: `python scripts/load_duckdb.py --db-file data/db/stock.duckdb`
- **Run dashboard**: `streamlit run dashboard.py`

### Caching and performance
- Streamlit uses `@st.cache_resource` for the DuckDB connection and `@st.cache_data` for symbol lists/data loading, reducing repeated I/O.

### Repository layout
- `lib/clients/nse_client.py`: NSE API integration and CSV writing for stocks/indices.
- `scripts/`: Container-friendly command entrypoints (`fetch_stocks.py`, `fetch_indices.py`, `init_localdb.py`, `populate_universe.py`, `load_duckdb.py`, `dashboard.py`).
- `init_local_db.py`: Core schema creation logic (wrapped by `scripts/init_localdb.py`).
- `populate_local_db.py`: Core universe population logic (wrapped by `scripts/populate_universe.py`).
- `load_to_duckdb.py`: Incremental loads from cache CSVs to DuckDB.
- `dashboard.py`: Streamlit application (query, compute, visualize).
- `data/cache/price_history/`: Per-stock CSVs.
- `data/cache/index_history/`: Per-index CSVs.
- `data/universe/`: Universe CSVs (e.g., `nse_nifty500.csv`).
- `data/db/stock.duckdb`: DuckDB database (generated). 