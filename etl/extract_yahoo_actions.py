import pandas as pd
import yfinance as yf
from etl.config import load_sources
from etl.db import write_df

def fetch_yahoo_actions():
    """
    Pull dividends & splits from Yahoo Finance for each instrument that has a 'yahoo' mapping.
    Writes into raw_yf_actions.
    """
    cfg = load_sources()
    frames = []

    for inst in cfg["instruments"]:
        ysym = inst.get("yahoo")
        if not ysym:
            continue

        instrument_id = inst["instrument_id"]

        try:
            t = yf.Ticker(ysym)
            actions = t.actions  # DataFrame with index=Date, columns: Dividends, Stock Splits

            if actions is None or actions.empty:
                # Not all tickers have actions; skip quietly
                continue

            # Normalize
            df = actions.rename(
                columns={"Dividends": "dividend", "Stock Splits": "split_coeff"}
            ).reset_index().rename(columns={"Date": "action_date"})

            # Ensure dtypes
            df["action_date"] = pd.to_datetime(df["action_date"]).dt.date

            # Some entries can be 0.0; keep them as-is (we'll filter in core)
            df["source"] = "yahoo"
            df["instrument_id"] = instrument_id
            df["vendor_symbol"] = ysym

            frames.append(df[["source","instrument_id","vendor_symbol","action_date","dividend","split_coeff"]])

        except Exception as e:
            print(f"[yahoo-actions] {instrument_id} ({ysym}) error: {e}")

    if frames:
        out = pd.concat(frames, ignore_index=True)
        n = write_df(out, table="raw_yf_actions", schema="public")
        print(f"[yahoo-actions] inserted rows: {n}")
    else:
        print("[yahoo-actions] no actions fetched")
