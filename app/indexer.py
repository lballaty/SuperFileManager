# app/indexer.py — incremental + checksums + cancel + knobs

import os, time, stat, sqlite3, threading
from blake3 import blake3
from . import extract

# Defaults (overridable via function args)
LARGE_MB_DEFAULT       = 64
SAMPLE_HEAD_MB_DEFAULT = 4
SAMPLE_TAIL_MB_DEFAULT = 4
SAMPLE_STRIDE_DEFAULT  = 0.01   # 1%


def _get_created_at(st) -> int | None:
    try:
        return int(st.st_birthtime)    # macOS
    except AttributeError:
        pass
    if os.name == "nt":
        return int(st.st_ctime)        # Windows creation time
    return None                         # Linux often unavailable

def _blake3_file(path, size, sample=True,
                 large_mb=LARGE_MB_DEFAULT,
                 head_mb=SAMPLE_HEAD_MB_DEFAULT,
                 tail_mb=SAMPLE_TAIL_MB_DEFAULT,
                 stride=SAMPLE_STRIDE_DEFAULT):
    h = blake3()
    with open(path, "rb") as f:
        if (size <= large_mb*1024*1024) or not sample:
            for buf in iter(lambda: f.read(1<<20), b""): h.update(buf)
        else:
            f.seek(0); h.update(f.read(head_mb*1024*1024))
            start = head_mb*1024*1024
            end   = max(0, size - tail_mb*1024*1024)
            step  = max(1, int(size * stride))
            pos = start
            while pos < end:
                f.seek(pos); h.update(f.read(1<<20))
                pos += step
            f.seek(max(0, size - tail_mb*1024*1024))
            h.update(f.read(tail_mb*1024*1024))
    return h.hexdigest()

def _get_row(cur: sqlite3.Cursor, path: str):
    cur.execute("""SELECT id,size,mtime,inode,blake3,hash_checked_at,last_indexed_at
                   FROM files WHERE path=?""", (path,))
    return cur.fetchone()

def _upsert_meta(cur: sqlite3.Cursor, path: str, st) -> int:
    cur.execute(
        "INSERT INTO files(path,size,mtime,inode,status,last_seen) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, "
        "inode=excluded.inode, last_seen=excluded.last_seen, status='ok'",
        (path, st.st_size, int(st.st_mtime), f"{st.st_ino}", "ok", int(time.time()))
    )
    cur.execute("SELECT id FROM files WHERE path=?", (path,))
    return cur.fetchone()[0]

def index_root(
    con: sqlite3.Connection,
    root: str,
    exclude_dirs: list[str],
    *,
    max_read_bytes: int = 200_000,
    progress_cb=None,
    batch: int = 200,
    prune_missing: bool = False,
    # knobs
    reindex_days: int = 14,
    verify_hash_days: int = 7,
    force_full_hash_large: bool = False,
    # cancel
    stop_event: threading.Event | None = None,
):
    cur = con.cursor()
    now = int(time.time())
    reindex_sec = max(0, reindex_days) * 86400
    verify_sec  = max(0, verify_hash_days) * 86400

    files_seen=0; files_indexed=0; chunks_written=0; t0=time.time()
    cancelled = False

    existing_paths = set()
    if prune_missing:
        cur.execute("SELECT path FROM files WHERE path LIKE ? || '%'", (os.path.abspath(root),))
        existing_paths = {r[0] for r in cur.fetchall()}

    for r, dirs, fnames in os.walk(root):
        if any(x in r for x in exclude_dirs): continue
        if stop_event and stop_event.is_set():
            cancelled = True
            break
        abased = os.path.abspath(r)
        for fn in fnames:
            if stop_event and stop_event.is_set():
                cancelled = True
                break
            fp = os.path.join(abased, fn)
            try:
                st = os.stat(fp, follow_symlinks=False)
                if stat.S_ISDIR(st.st_mode): continue

                row = _get_row(cur, fp)
                unchanged_meta = bool(row and row[1] == st.st_size and row[2] == int(st.st_mtime) and row[3] == f"{st.st_ino}")
                last_indexed_at = row[6] if row else None
                age_ok = (last_indexed_at is not None) and (reindex_sec == 0 or (now - last_indexed_at) < reindex_sec)

                if unchanged_meta and age_ok:
                    cur.execute("UPDATE files SET last_seen=?, status='ok' WHERE path=?", (now, fp))
                else:
                    fid = _upsert_meta(cur, fp, st)

                    ca = _get_created_at(st)
                    if ca is not None:
                        cur.execute("UPDATE files SET created_at=? WHERE id=?", (ca, fid))


                    # checksum lane
                    same_hash = False
                    need_verify = True
                    if row and row[5]:
                        need_verify = (verify_sec == 0) or ((now - int(row[5])) >= verify_sec) or (not unchanged_meta)

                    if need_verify:
                        digest = _blake3_file(
                            fp, st.st_size,
                            sample=not force_full_hash_large,
                        )
                        if row and row[4] and row[4] == digest and age_ok:
                            same_hash = True
                        cur.execute("UPDATE files SET blake3=?, hash_checked_at=? WHERE path=?", (digest, now, fp))
                    else:
                        if row and row[4] and age_ok:
                            same_hash = True

                    if same_hash and age_ok:
                        pass  # metadata already updated above
                    else:
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
                                chunks_written += (ord_ + 1) if ord_ >= 0 else 0
                        cur.execute("UPDATE files SET last_indexed_at=? WHERE path=?", (now, fp))
                    files_indexed += 1

                files_seen += 1
                if prune_missing and fp in existing_paths:
                    existing_paths.remove(fp)

                if files_seen % batch == 0:
                    con.commit()
                    if progress_cb:
                        progress_cb({"files_seen": files_seen, "files_indexed": files_indexed,
                                     "chunks": chunks_written, "secs": round(time.time()-t0,1)})
            except Exception:
                cur.execute("INSERT OR IGNORE INTO files(path,status,last_seen) VALUES(?,?,?)", (fp, "error", now))
                continue
        if cancelled: break

    if prune_missing and existing_paths:
        ph = ",".join("?"*len(existing_paths))
        cur.execute(f"DELETE FROM files WHERE path IN ({ph})", tuple(existing_paths))

    con.commit()
    if progress_cb:
        progress_cb({"files_seen": files_seen, "files_indexed": files_indexed,
                     "chunks": chunks_written, "secs": round(time.time()-t0,1),
                     "done": True, "cancelled": cancelled})
    return {"files_seen": files_seen, "files_indexed": files_indexed,
            "chunks": chunks_written, "cancelled": cancelled}
