from etl.extract_yahoo import fetch_yahoo
from etl.extract_stooq import fetch_stooq
from etl.extract_yahoo_actions import fetch_yahoo_actions  

def main():
    print("[1/3] Yahoo prices…")
    try:
        fetch_yahoo()
    except Exception as e:
        print(f"[INGEST] Yahoo prices failed: {e}")

    print("[2/3] Stooq prices…")
    try:
        fetch_stooq()
    except Exception as e:
        print(f"[INGEST] Stooq failed: {e}")

    print("[3/3] Yahoo corporate actions…")
    try:
        fetch_yahoo_actions()
    except Exception as e:
        print(f"[INGEST] Yahoo actions failed: {e}")

    print("Ingestion finished.")

if __name__ == "__main__":
    main()
