# app/indexer.py
import os, time, stat, sqlite3
from . import db, extract

def upsert_file(cur: sqlite3.Cursor, path: str, st) -> int:
    cur.execute(
        "INSERT INTO files(path,size,mtime,inode,status,last_seen) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET size=excluded.size,mtime=excluded.mtime,"
        "inode=excluded.inode,last_seen=excluded.last_seen",
        (path, st.st_size, int(st.st_mtime), f"{st.st_ino}", "ok", int(time.time()))
    )
    cur.execute("SELECT id FROM files WHERE path=?", (path,))
    return cur.fetchone()[0]

def index_root(con: sqlite3.Connection, root: str, exclude_dirs: list[str],
               max_read_bytes: int=200_000, progress_cb=None, batch: int=200):
    cur = con.cursor()
    files=chunks=0
    t0 = time.time()
    for r, dirs, fnames in os.walk(root):
        if any(x in r for x in exclude_dirs): continue
        for fn in fnames:
            fp = os.path.join(r, fn)
            try:
                st = os.stat(fp, follow_symlinks=False)
                if stat.S_ISDIR(st.st_mode): continue
                fid = upsert_file(cur, fp, st)
                if extract.is_textable(fp):
                    text = extract.read_text(fp, max_read_bytes)
                    if text:
                        cur.execute("DELETE FROM chunks WHERE file_id=?", (fid,))
                        ord_ = -1
                        for ord_, seg, b0, b1 in extract.chunk(text):
                            cur.execute("INSERT INTO chunks(file_id,ord,text,bytes_from,bytes_to) VALUES(?,?,?,?,?)",
                                        (fid, ord_, seg, b0, b1))
                            cur.execute("INSERT INTO fts(rowid,text) VALUES(NULL,?)", (seg,))
                            rid = cur.lastrowid
                            cur.execute("SELECT id FROM chunks WHERE file_id=? AND ord=?", (fid, ord_))
                            cid = cur.fetchone()[0]
                            cur.execute("INSERT OR REPLACE INTO fts_map(rowid,chunk_id) VALUES(?,?)", (rid, cid))
                        chunks += (ord_ + 1) if ord_ >= 0 else 0
                files += 1
                if files % batch == 0:
                    con.commit()
                    if progress_cb:
                        dt = time.time()-t0
                        progress_cb({"files": files, "chunks": chunks, "secs": round(dt,1)})
            except Exception:
                continue
    con.commit()
    if progress_cb:
        dt = time.time()-t0
        progress_cb({"files": files, "chunks": chunks, "secs": round(dt,1), "done": True})
    return {"files": files, "chunks": chunks}

