import time, requests, pandas as pd
from etl.config import load_sources, ALPHAVANTAGE_API_KEY
from etl.db import write_df

RATE_SLEEP_SEC = 15  # free tier: <=5 calls/min → ~12s; give ourselves margin

def _rows_from_ts(instrument_id: str, vendor_symbol: str, ts: dict) -> list[dict]:
    rows = []
    for d, v in ts.items():
        rows.append({
            "source": "alphavantage",
            "instrument_id": instrument_id,         # canonical
            "vendor_symbol": vendor_symbol,         # what AV expects
            "trade_date": d,
            "open": float(v.get("1. open", "nan")),
            "high": float(v.get("2. high", "nan")),
            "low": float(v.get("3. low", "nan")),
            "close": float(v.get("4. close", "nan")),
            "adj_close": float(v.get("5. adjusted close", "nan")),
            "volume": int(float(v.get("6. volume", "0"))),
            "dividend": float(v.get("7. dividend amount", "0")),
            "split_coeff": float(v.get("8. split coefficient", "1")),
        })
    return rows

def fetch_av():
    if not ALPHAVANTAGE_API_KEY:
        print("[alphavantage] No API key; skipping.")
        return

    cfg = load_sources()
    frames = []

    for inst in cfg["instruments"]:
        av_sym = inst.get("alphavantage")
        if not av_sym:
            continue  # skip non-US / not mapped for AV

        instrument_id = inst["instrument_id"]
        url = (
            "https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY_ADJUSTED&symbol={av_sym}"
            f"&outputsize=full&apikey={ALPHAVANTAGE_API_KEY}"
        )

        try:
            r = requests.get(url, timeout=30)
            j = r.json()

            # Helpful diagnostics
            if "Error Message" in j:
                print(f"[alphavantage] {instrument_id} ({av_sym}) ERROR: {j['Error Message']}")
                time.sleep(RATE_SLEEP_SEC)
                continue
            if "Note" in j or "Information" in j:
                note = j.get("Note") or j.get("Information")
                print(f"[alphavantage] Rate/Info for {instrument_id} ({av_sym}): {note}")
                # hit rate limit → back off and retry once
                time.sleep(RATE_SLEEP_SEC + 5)
                r = requests.get(url, timeout=30)
                j = r.json()

            ts = j.get("Time Series (Daily)") or j.get("Time Series (Digital Currency Daily)") or {}
            if not ts:
                print(f"[alphavantage] {instrument_id} ({av_sym}) returned no time series; skipping.")
                time.sleep(RATE_SLEEP_SEC)
                continue

            rows = _rows_from_ts(instrument_id, av_sym, ts)
            frames.append(pd.DataFrame(rows))

            time.sleep(RATE_SLEEP_SEC)  # respect free tier
        except Exception as e:
            print(f"[alphavantage] {instrument_id} ({av_sym}) exception: {e}")
            time.sleep(RATE_SLEEP_SEC)

    if frames:
        out = pd.concat(frames, ignore_index=True)
        # ensure date dtype
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
        n = write_df(out, table="raw_av_adjusted", schema="public")
        print(f"[alphavantage] inserted rows: {n}")
    else:
        print("[alphavantage] no data fetched")
