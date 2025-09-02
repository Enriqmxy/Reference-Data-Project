import io, requests, pandas as pd
from etl.config import load_sources
from etl.db import write_df

def fetch_stooq():
    cfg = load_sources()
    frames = []
    for inst in cfg["instruments"]:
        stooq_sym = inst.get("stooq")
        if not stooq_sym:
            continue  # skip non-US or unmapped instruments
        instrument_id = inst["instrument_id"]
        url = f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200 or "Date,Open,High,Low,Close,Volume" not in r.text:
                continue
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty:
                continue
            df = df.rename(columns={"Date":"trade_date","Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
            df["source"] = "stooq"
            df["instrument_id"] = instrument_id
            df["vendor_symbol"] = stooq_sym
            frames.append(df[["source","instrument_id","vendor_symbol","trade_date","open","high","low","close","volume"]])
        except Exception as e:
            print(f"[stooq] {instrument_id} ({stooq_sym}) error: {e}")

    if frames:
        out = pd.concat(frames, ignore_index=True)
        n = write_df(out, table="raw_stooq_prices", schema="public")
        print(f"[stooq] inserted rows: {n}")
    else:
        print("[stooq] no data fetched")
