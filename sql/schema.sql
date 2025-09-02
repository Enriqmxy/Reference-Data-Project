-- RAW (vendor landing)
CREATE TABLE IF NOT EXISTS raw_yf_prices (
  source TEXT, symbol TEXT, trade_date DATE,
  "open" DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION,
  close DOUBLE PRECISION, adj_close DOUBLE PRECISION,
  volume BIGINT, currency TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_yf_symbol_date ON raw_yf_prices(symbol, trade_date);

CREATE TABLE IF NOT EXISTS raw_stooq_prices (
  source TEXT, symbol TEXT, trade_date DATE,
  "open" DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION,
  close DOUBLE PRECISION, volume BIGINT
);
CREATE INDEX IF NOT EXISTS ix_raw_stooq_symbol_date ON raw_stooq_prices(symbol, trade_date);

CREATE TABLE IF NOT EXISTS raw_av_adjusted (
  source TEXT, symbol TEXT, trade_date DATE,
  "open" DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION,
  close DOUBLE PRECISION, adj_close DOUBLE PRECISION, volume BIGINT,
  dividend DOUBLE PRECISION, split_coeff DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ix_raw_av_symbol_date ON raw_av_adjusted(symbol, trade_date);

CREATE TABLE IF NOT EXISTS raw_sec_ticker_cik (
  symbol TEXT, cik TEXT, title TEXT
);

-- CORE / GOLDEN
CREATE TABLE IF NOT EXISTS core_dim_instrument (
  instrument_id TEXT PRIMARY KEY,   -- MVP: ticker; we can add surrogate later
  source_primary TEXT,
  currency TEXT
);

CREATE TABLE IF NOT EXISTS core_fact_prices_eod (
  instrument_id TEXT,
  trade_date DATE,
  "open" DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION,
  close DOUBLE PRECISION, adj_close DOUBLE PRECISION,
  volume BIGINT, source_primary TEXT,
  PRIMARY KEY (instrument_id, trade_date)
);

CREATE TABLE IF NOT EXISTS core_fact_corporate_actions (
  instrument_id TEXT,
  action_date DATE,
  action_type TEXT,          -- 'DIV' or 'SPLIT'
  cash_amount DOUBLE PRECISION,
  split_coeff DOUBLE PRECISION
);
