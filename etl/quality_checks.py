from sqlalchemy import create_engine, text
from etl.config import db_url

def run_checks():
    eng = create_engine(db_url(), future=True)
    issues = []
    with eng.begin() as con:
        dup = con.execute(text("""
            SELECT COUNT(*) FROM (
              SELECT instrument_id, trade_date, COUNT(*) c
              FROM core_fact_prices_eod
              GROUP BY 1,2 HAVING COUNT(*)>1
            ) s
        """)).scalar()
        if dup and dup > 0:
            issues.append(f"Duplicate keys in core_fact_prices_eod: {dup}")

        nn = con.execute(text("""
            SELECT COUNT(*) FROM core_fact_prices_eod
            WHERE instrument_id IS NULL OR trade_date IS NULL OR close IS NULL
        """)).scalar()
        if nn and nn > 0:
            issues.append(f"Nulls in key fields: {nn}")

        neg = con.execute(text("""
            SELECT COUNT(*) FROM core_fact_prices_eod
            WHERE "open"<=0 OR high<=0 OR low<=0 OR close<=0
        """)).scalar()
        if neg and neg > 0:
            issues.append(f"Non-positive OHLC rows: {neg}")
    return issues
