from sqlalchemy import create_engine, text
from etl.config import db_url

SQL_BUILD_FACT = """
WITH unioned AS (
    -- Alpha Vantage (has adj_close)
    SELECT instrument_id, trade_date, "open", high, low, close, adj_close, volume, 'alphavantage' AS src
    FROM raw_av_adjusted

    UNION ALL

    -- Stooq (NO adj_close) -> fill with NULL cast to match type
    SELECT instrument_id, trade_date, "open", high, low, close,
           NULL::double precision AS adj_close,
           volume, 'stooq' AS src
    FROM raw_stooq_prices

    UNION ALL

    -- Yahoo (has adj_close)
    SELECT instrument_id, trade_date, "open", high, low, close, adj_close, volume, 'yahoo' AS src
    FROM raw_yf_prices
),
ranked AS (
    SELECT *,
           CASE src
             WHEN 'alphavantage' THEN 1
             WHEN 'stooq' THEN 2
             WHEN 'yahoo' THEN 3
             ELSE 9
           END AS pri
    FROM unioned
),
chosen AS (
    SELECT DISTINCT ON (instrument_id, trade_date)
           instrument_id, trade_date, "open", high, low, close, adj_close, volume, src AS source_primary
    FROM ranked
    ORDER BY instrument_id, trade_date, pri
)
INSERT INTO core_fact_prices_eod (instrument_id, trade_date, "open", high, low, close, adj_close, volume, source_primary)
SELECT instrument_id, trade_date, "open", high, low, close, adj_close, volume, source_primary
FROM chosen
ON CONFLICT (instrument_id, trade_date) DO UPDATE
SET "open" = EXCLUDED."open",
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume = EXCLUDED.volume,
    source_primary = EXCLUDED.source_primary;
"""

SQL_BUILD_DIM = """
INSERT INTO core_dim_instrument (instrument_id, source_primary, currency)
SELECT instrument_id,
       'alphavantage>stooq>yahoo' AS source_primary,
       NULL::text AS currency
FROM (
    SELECT instrument_id FROM raw_yf_prices
    UNION
    SELECT instrument_id FROM raw_stooq_prices
    UNION
    SELECT instrument_id FROM raw_av_adjusted
) u
ON CONFLICT (instrument_id) DO UPDATE
SET source_primary = EXCLUDED.source_primary;
"""

SQL_BUILD_CA = """
-- Populate corporate actions from Yahoo (dividends + splits)
INSERT INTO core_fact_corporate_actions (instrument_id, action_date, action_type, cash_amount, split_coeff)
SELECT
    instrument_id,
    action_date,
    CASE
      WHEN split_coeff IS NOT NULL AND split_coeff <> 0 AND split_coeff <> 1 THEN 'SPLIT'
      WHEN dividend IS NOT NULL AND dividend <> 0 THEN 'DIV'
      ELSE NULL
    END AS action_type,
    NULLIF(dividend, 0) AS cash_amount,
    CASE
      WHEN split_coeff IS NOT NULL AND split_coeff <> 0 AND split_coeff <> 1 THEN split_coeff
      ELSE NULL
    END AS split_coeff
FROM raw_yf_actions
WHERE (split_coeff IS NOT NULL AND split_coeff NOT IN (0,1))
   OR (dividend IS NOT NULL AND dividend <> 0)
ON CONFLICT DO NOTHING;
"""

def build_core():
    eng = create_engine(db_url(), future=True)
    with eng.begin() as con:
        con.execute(text(SQL_BUILD_FACT))
        con.execute(text(SQL_BUILD_DIM))
        con.execute(text(SQL_BUILD_CA))
    print("[core] golden tables refreshed.")
