from dotenv import load_dotenv
import os, yaml

load_dotenv(r"D:\Enriq\Ref data project\ref-data golden\conf\.env")

def db_url():
    return (f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
            f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}")

def load_sources():
    with open(r"D:\Enriq\Ref data project\ref-data golden\conf\sources.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
LOOKBACK_YEARS = int(os.getenv("LOOKBACK_YEARS", "2"))
