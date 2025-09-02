from etl.transform_to_core import build_core
from etl.quality_checks import run_checks  # optional if you added it

def main():
    print("[core] Building golden copy...")
    build_core()
    try:
        issues = run_checks()
        if not issues:
            print("[DQ] All checks passed ✅")
        else:
            print("[DQ] Issues:")
            for issue in issues:
                print(" - " + issue)
    except Exception as e:
        print(f"[DQ] Skipped or failed: {e}")
    print("[core] Done.")

if __name__ == "__main__":
    main()
