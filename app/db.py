import sqlite3, pathlib

PRAGMAS = [
 "PRAGMA journal_mode=WAL;",
 "PRAGMA synchronous=NORMAL;",
 "PRAGMA temp_store=MEMORY;",
 "PRAGMA cache_size=-80000;",
 "PRAGMA busy_timeout=5000;"
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS files(
  id INTEGER PRIMARY KEY,
  path TEXT UNIQUE,
  size INTEGER, mtime INTEGER, inode TEXT,
  mime TEXT, sha1 TEXT, status TEXT, last_seen INTEGER
);
CREATE TABLE IF NOT EXISTS chunks(
  id INTEGER PRIMARY KEY,
  file_id INTEGER REFERENCES files(id) ON DELETE CASCADE,
  ord INTEGER, text TEXT, bytes_from INTEGER, bytes_to INTEGER
);
CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
  text, tokenize='porter', content='', prefix=2
);
CREATE TABLE IF NOT EXISTS fts_map(rowid INTEGER PRIMARY KEY, chunk_id INTEGER UNIQUE);
CREATE INDEX IF NOT EXISTS idx_chunks_file ON chunks(file_id);
"""

def connect(db_path: str, *, check_same_thread: bool = True, timeout: float = 30.0) -> sqlite3.Connection:
    pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path, check_same_thread=check_same_thread, timeout=timeout)
    for p in PRAGMAS: con.execute(p)
    return con

def init(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()
    

def _ensure_column(con, table, col, decl):
    cur = con.execute(f"PRAGMA table_info({table})")
    have = any(r[1] == col for r in cur.fetchall())
    if not have:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

def migrate(con):
    # new metadata
    _ensure_column(con, "files", "blake3", "TEXT")
    _ensure_column(con, "files", "hash_checked_at", "INTEGER")
    _ensure_column(con, "files", "last_indexed_at", "INTEGER")
    con.commit()


