import pandas as pd
from sqlalchemy import create_engine
from etl.config import db_url

def write_df(df: pd.DataFrame, table: str, schema: str):
    if df is None or df.empty:
        return 0
    eng = create_engine(db_url(), future=True)
    df.to_sql(table, eng, schema=schema, if_exists="append", index=False)
    return len(df)
