import sqlite3, pathlib, os, json

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
  size INTEGER, mtime INTEGER, created_at INTEGER, inode TEXT,
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
    _ensure_column(con, "files", "blake3", "TEXT")
    _ensure_column(con, "files", "hash_checked_at", "INTEGER")
    _ensure_column(con, "files", "last_indexed_at", "INTEGER")
    _ensure_column(con, "files", "created_at", "INTEGER")  # NEW

    con.execute("CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at)")
    con.commit()



def counts_for_root(con, root: str) -> dict:
    root_abs = os.path.abspath(root)
    sep = "\\" if os.name == "nt" else "/"
    prefix = root_abs if root_abs.endswith(sep) else root_abs + sep
    like = prefix + "%"

    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE path LIKE ?", (like,))
    files_total = cur.fetchone()[0]

    cur.execute("""SELECT COUNT(DISTINCT c.file_id)
                   FROM chunks c JOIN files f ON f.id = c.file_id
                   WHERE f.path LIKE ?""", (like,))
    files_text = cur.fetchone()[0]

    cur.execute("""SELECT COUNT(*)
                   FROM chunks c JOIN files f ON f.id = c.file_id
                   WHERE f.path LIKE ?""", (like,))
    chunks = cur.fetchone()[0]

    cur.execute("SELECT MAX(last_indexed_at) FROM files WHERE path LIKE ?", (like,))
    last_idx = cur.fetchone()[0]

    return {"files_total": files_total, "files_text": files_text,
            "chunks": chunks, "last_indexed_at": last_idx}

# settings helpers

def ensure_settings(con):
    con.execute("CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT)")
    con.commit()

def get_setting(con, k, default=None):
    ensure_settings(con)
    row = con.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return json.loads(row[0]) if row else default

def set_setting(con, k, value):
    ensure_settings(con)
    con.execute(
        "INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (k, json.dumps(value)),
    )
    con.commit()
