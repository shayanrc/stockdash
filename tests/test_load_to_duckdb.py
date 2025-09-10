import duckdb
import pandas as pd
import pytest
from pathlib import Path

import load_to_duckdb as ltd


def create_duckdb_with_schemas(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=False)
    con.execute("""
    CREATE TABLE IF NOT EXISTS universe_stocks (
      "Company Name" VARCHAR NOT NULL,
      "Industry"     VARCHAR,
      "Symbol"       VARCHAR NOT NULL,
      "Exchange"     VARCHAR DEFAULT 'NSE',
      "code"         VARCHAR,
      PRIMARY KEY ("Symbol", "Exchange")
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS universe_indexes (
      "Index"    VARCHAR NOT NULL,
      "Exchange" VARCHAR DEFAULT 'NSE',
      "Type"     VARCHAR,
      PRIMARY KEY ("Index", "Exchange")
    );
    """)
    con.execute("""
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
    """)
    con.execute("""
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
    """)
    return con


def write_stock_csv(dir_path: Path, symbol: str, rows):
    dir_path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=[
        "DATE","OPEN","HIGH","LOW","PREVCLOSE","LTP","CLOSE","VWAP","VOLUME","VALUE","NOOFTRADES","SYMBOL","SERIES"
    ])
    df.to_csv(dir_path / f"{symbol}.csv", index=False)


def write_index_csv(dir_path: Path, index_name: str, rows):
    dir_path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=[
        "Date","INDEX_NAME","OPEN_INDEX_VAL","HIGH_INDEX_VAL","LOW_INDEX_VAL","Close","TRADED_QTY","TURN_OVER"
    ])
    df.to_csv(dir_path / f"{index_name}.csv", index=False)


def test_clean_col_names_sanitizes_columns():
    df = pd.DataFrame(columns=["Date", "NO OF TRADES", "Turnover (Cr)"])
    out = ltd.clean_col_names(df.copy())
    assert out.columns.tolist() == ["Date", "NOOFTRADES", "TurnoverCr"]


def test_get_latest_date_none_and_max():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE stock_prices (date DATE, symbol VARCHAR, exchange VARCHAR)")
    con.execute("INSERT INTO stock_prices VALUES ('2024-01-01','AAA','NSE'), ('2024-01-03','AAA','NSE'), ('2024-01-02','BBB','NSE')")
    assert ltd.get_latest_date(con, "stock_prices", "symbol", "ZZZ") is None
    d = ltd.get_latest_date(con, "stock_prices", "symbol", "AAA")
    assert pd.to_datetime(d) == pd.Timestamp("2024-01-03")


def test_load_stock_data_incremental_and_exchange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    con = create_duckdb_with_schemas(db_path)

    price_dir = tmp_path / "data" / "cache" / "price_history"
    symbol = "TESTCO"

    write_stock_csv(price_dir, symbol, [
        {"DATE":"2024-01-01","OPEN":10,"HIGH":12,"LOW":9,"PREVCLOSE":9.5,"LTP":11,"CLOSE":11,"VWAP":10.5,"VOLUME":1000,"VALUE":10500.0,"NOOFTRADES":10,"SYMBOL":symbol,"SERIES":"EQ"},
        {"DATE":"2024-01-02","OPEN":11,"HIGH":13,"LOW":10,"PREVCLOSE":11,"LTP":12,"CLOSE":12,"VWAP":11.2,"VOLUME":1200,"VALUE":13440.0,"NOOFTRADES":11,"SYMBOL":symbol,"SERIES":"EQ"},
    ])

    ltd.load_stock_data(con)
    n = con.execute("SELECT COUNT(*) FROM stock_prices WHERE symbol = ?", [symbol]).fetchone()[0]
    assert n == 2

    write_stock_csv(price_dir, symbol, [
        {"DATE":"2024-01-01","OPEN":10,"HIGH":12,"LOW":9,"PREVCLOSE":9.5,"LTP":11,"CLOSE":11,"VWAP":10.5,"VOLUME":1000,"VALUE":10500.0,"NOOFTRADES":10,"SYMBOL":symbol,"SERIES":"EQ"},
        {"DATE":"2024-01-02","OPEN":11,"HIGH":13,"LOW":10,"PREVCLOSE":11,"LTP":12,"CLOSE":12,"VWAP":11.2,"VOLUME":1200,"VALUE":13440.0,"NOOFTRADES":11,"SYMBOL":symbol,"SERIES":"EQ"},
        {"DATE":"2024-01-03","OPEN":12,"HIGH":14,"LOW":11,"PREVCLOSE":12,"LTP":13,"CLOSE":13,"VWAP":12.1,"VOLUME":1300,"VALUE":15730.0,"NOOFTRADES":12,"SYMBOL":symbol,"SERIES":"EQ"},
    ])

    ltd.load_stock_data(con)
    n = con.execute("SELECT COUNT(*) FROM stock_prices WHERE symbol = ?", [symbol]).fetchone()[0]
    assert n == 3

    ex = con.execute("SELECT DISTINCT exchange FROM stock_prices WHERE symbol = ?", [symbol]).fetchall()
    assert ex == [('NSE',)]


def test_load_stock_data_no_new_rows_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    con = create_duckdb_with_schemas(db_path)

    price_dir = tmp_path / "data" / "cache" / "price_history"
    symbol = "NONEW"
    write_stock_csv(price_dir, symbol, [
        {"DATE":"2024-01-05","OPEN":100,"HIGH":101,"LOW":99,"PREVCLOSE":98,"LTP":100,"CLOSE":100,"VWAP":99.9,"VOLUME":10,"VALUE":999.0,"NOOFTRADES":1,"SYMBOL":symbol,"SERIES":"EQ"},
    ])

    ltd.load_stock_data(con)
    ltd.load_stock_data(con)
    out = capsys.readouterr().out
    assert f"No new data for stock: {symbol}" in out


def test_load_index_data_dedup_and_incremental(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    con = create_duckdb_with_schemas(db_path)

    index_dir = tmp_path / "data" / "cache" / "index_history"
    index_name = "TESTINDEX"

    write_index_csv(index_dir, index_name, [
        {"Date":"2024-01-01","INDEX_NAME":index_name,"OPEN_INDEX_VAL":100,"HIGH_INDEX_VAL":110,"LOW_INDEX_VAL":90,"Close":105,"TRADED_QTY":100000,"TURN_OVER":1000000},
        {"Date":"2024-01-01","INDEX_NAME":index_name,"OPEN_INDEX_VAL":100,"HIGH_INDEX_VAL":110,"LOW_INDEX_VAL":90,"Close":105,"TRADED_QTY":100000,"TURN_OVER":1000000},
        {"Date":"2024-01-02","INDEX_NAME":index_name,"OPEN_INDEX_VAL":106,"HIGH_INDEX_VAL":111,"LOW_INDEX_VAL":95,"Close":108,"TRADED_QTY":110000,"TURN_OVER":1100000},
    ])

    ltd.load_index_data(con)
    n = con.execute("SELECT COUNT(*) FROM index_prices WHERE symbol = ?", [index_name]).fetchone()[0]
    assert n == 2

    write_index_csv(index_dir, index_name, [
        {"Date":"2024-01-01","INDEX_NAME":index_name,"OPEN_INDEX_VAL":100,"HIGH_INDEX_VAL":110,"LOW_INDEX_VAL":90,"Close":105,"TRADED_QTY":100000,"TURN_OVER":1000000},
        {"Date":"2024-01-02","INDEX_NAME":index_name,"OPEN_INDEX_VAL":106,"HIGH_INDEX_VAL":111,"LOW_INDEX_VAL":95,"Close":108,"TRADED_QTY":110000,"TURN_OVER":1100000},
        {"Date":"2024-01-03","INDEX_NAME":index_name,"OPEN_INDEX_VAL":109,"HIGH_INDEX_VAL":115,"LOW_INDEX_VAL":100,"Close":112,"TRADED_QTY":120000,"TURN_OVER":1200000},
    ])
    ltd.load_index_data(con)
    n = con.execute("SELECT COUNT(*) FROM index_prices WHERE symbol = ?", [index_name]).fetchone()[0]
    assert n == 3


def test_load_index_data_no_new_rows_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    con = create_duckdb_with_schemas(db_path)

    index_dir = tmp_path / "data" / "cache" / "index_history"
    index_name = "NOIDXCHANGE"
    write_index_csv(index_dir, index_name, [
        {"Date":"2024-02-01","INDEX_NAME":index_name,"OPEN_INDEX_VAL":200,"HIGH_INDEX_VAL":210,"LOW_INDEX_VAL":190,"Close":205,"TRADED_QTY":1000,"TURN_OVER":2000},
    ])

    ltd.load_index_data(con)
    ltd.load_index_data(con)
    out = capsys.readouterr().out
    assert f"No new data for index: {index_name}" in out


def test_main_runs_and_updates_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "db" / "stock.duckdb"
    con = create_duckdb_with_schemas(db_path)
    con.close()

    price_dir = tmp_path / "data" / "cache" / "price_history"
    write_stock_csv(price_dir, "MAINCO", [
        {"DATE":"2024-03-01","OPEN":1,"HIGH":2,"LOW":0.5,"PREVCLOSE":0.9,"LTP":1.5,"CLOSE":1.6,"VWAP":1.2,"VOLUME":100,"VALUE":120.0,"NOOFTRADES":5,"SYMBOL":"MAINCO","SERIES":"EQ"},
    ])
    index_dir = tmp_path / "data" / "cache" / "index_history"
    write_index_csv(index_dir, "MAININDEX", [
        {"Date":"2024-03-01","INDEX_NAME":"MAININDEX","OPEN_INDEX_VAL":300,"HIGH_INDEX_VAL":310,"LOW_INDEX_VAL":295,"Close":305,"TRADED_QTY":5000,"TURN_OVER":9000},
    ])

    ltd.main(db_file=str(db_path))

    con2 = duckdb.connect(str(db_path))
    s = con2.execute("SELECT COUNT(*) FROM stock_prices WHERE symbol='MAINCO'").fetchone()[0]
    i = con2.execute("SELECT COUNT(*) FROM index_prices WHERE symbol='MAININDEX'").fetchone()[0]
    assert s == 1 and i == 1

    out = capsys.readouterr().out
    assert f"Database '{db_path}' has been successfully updated." in out 