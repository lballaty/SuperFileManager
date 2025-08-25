# save as check_fts5.py and run: python check_fts5.py
import sqlite3
con = sqlite3.connect(":memory:")
print("SQLite:", con.execute("select sqlite_version()").fetchone()[0])
con.execute("CREATE VIRTUAL TABLE t USING fts5(x)")
print("FTS5: OK")

