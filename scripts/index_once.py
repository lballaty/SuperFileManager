# scripts/index_once.py
import argparse, os
from app import db, indexer
from app.main import DB_PATH, EXCLUDES

p = argparse.ArgumentParser()
p.add_argument("--root", required=True)
p.add_argument("--prune-missing", action="store_true")
args = p.parse_args()

con = db.connect(DB_PATH, check_same_thread=False); db.init(con)
res = indexer.index_root(con, os.path.abspath(args.root), EXCLUDES,
                         progress_cb=lambda e: print(e),
                         batch=200, prune_missing=args.prune_missing)
print("DONE", res)
