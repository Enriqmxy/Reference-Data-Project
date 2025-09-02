# at top with the other imports
from etl.transform_to_core import build_core

# after your three extract calls
print("[4/4] Build golden copy…")
build_core()
print("All done.")
