from datetime import date, timedelta
import pandas as pd
import yfinance as yf
from etl.config import load_sources, LOOKBACK_YEARS
from etl.db import write_df

def _ensure_trade_date(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure there's a 'trade_date' column regardless of index name."""
    # If index already looks like a DatetimeIndex, reset with a fixed name
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index(names="trade_date")
    else:
        # Generic reset; then normalize the produced column name
        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "trade_date"})
        elif "date" in df.columns:
            df = df.rename(columns={"date": "trade_date"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "trade_date"})
        else:
            # As a last resort, create trade_date from the first column if it looks like dates
            first = df.columns[0]
            df = df.rename(columns={first: "trade_date"})
    return df

def _standardize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Map OHLCV columns to lower snake case, creating any missing ones as NaN."""
    rename_map = {
        "Open": "open", "open": "open",
        "High": "high", "high": "high",
        "Low":  "low",  "low":  "low",
        "Close":"close","close":"close",
        "Adj Close": "adj_close", "AdjClose": "adj_close", "adj close": "adj_close", "adj_close": "adj_close",
        "Volume":"volume","volume":"volume",
    }
    # Apply case-insensitive mapping
    lower_cols = {c: c for c in df.columns}
    for c in list(df.columns):
        if c in rename_map:
            lower_cols[c] = rename_map[c]
        elif c.lower() in rename_map:
            lower_cols[c] = rename_map[c.lower()]
    df = df.rename(columns=lower_cols)

    # Ensure required columns exist
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def fetch_yahoo():
    cfg = load_sources()
    instruments = cfg["instruments"]
    end = date.today()
    start = end - timedelta(days=365 * LOOKBACK_YEARS + 10)

    frames = []
    for inst in instruments:
        yahoo_sym = inst.get("yahoo")
        if not yahoo_sym:
            continue
        instrument_id = inst["instrument_id"]
        try:
            df = yf.download(
                yahoo_sym,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
            )

            if df is None or df.empty:
                print(f"[yahoo] {instrument_id} ({yahoo_sym}) returned empty — skipping")
                continue

            # Some tickers yield MultiIndex columns like ('Open','AAPL'); flatten to first level
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            # Ensure we get 'trade_date' column from whatever the index is
            df = _ensure_trade_date(df)

            # Standardize OHLCV names, create any missing
            df = _standardize_ohlcv(df)

            # Attach metadata & conform columns
            df["source"] = "yahoo"
            df["currency"] = None
            df["instrument_id"] = instrument_id         
            df["vendor_symbol"] = yahoo_sym

            frames.append(
                df[
                    [
                        "source",
                        "instrument_id",
                        "vendor_symbol",
                        "trade_date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "adj_close",
                        "volume",
                        "currency",
                    ]
                ]
            )
        except Exception as e:
            print(f"[yahoo] {instrument_id} ({yahoo_sym}) error: {e}")

    if frames:
        out = pd.concat(frames, ignore_index=True)
        n = write_df(out, table="raw_yf_prices", schema="public")
        print(f"[yahoo] inserted rows: {n}")
    else:
        print("[yahoo] no data fetched")
