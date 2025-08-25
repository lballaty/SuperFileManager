# app/searcher.py
import os, sqlite3, re
from typing import Iterable, Optional, List

def _norm_prefix(p: str) -> str:
    absp = os.path.abspath(p)
    if os.name == "nt":
        absp = os.path.normcase(absp)
    sep = "\\" if os.name == "nt" else "/"
    return absp if absp.endswith(sep) else absp + sep

def _in_scopes(path: str, prefixes: List[str]) -> bool:
    if not prefixes: return True
    norm = os.path.abspath(path)
    if os.name == "nt":
        norm = os.path.normcase(norm)
    return any(norm.startswith(pref) for pref in prefixes)

def fts(con: sqlite3.Connection, q: str, top_k: int = 200,
        path_prefixes: Optional[List[str]] = None) -> list[tuple]:
    """Return best chunk per file, filtered by optional directory scopes."""
    cur = con.cursor()
    cur.execute("""SELECT m.chunk_id, bm25(fts) AS score
                   FROM fts JOIN fts_map m ON m.rowid=fts.rowid
                   WHERE fts MATCH ? ORDER BY score LIMIT ?""", (q, top_k))
    ids = [r[0] for r in cur.fetchall()]
    if not ids: return []
    ph = ",".join("?" * len(ids))
    cur.execute(f"""SELECT c.id, c.ord, c.text, f.path
                    FROM chunks c JOIN files f ON c.file_id=f.id
                    WHERE c.id IN ({ph})""", ids)
    rows = cur.fetchall()
    order = {cid:i for i,cid in enumerate(ids)}
    rows.sort(key=lambda r: order.get(r[0], 1e9))

    prefixes = [_norm_prefix(p) for p in (path_prefixes or [])]
    best = {}
    for cid, ord_, text, path in rows:
        if not _in_scopes(path, prefixes): 
            continue
        best.setdefault(path, (cid, ord_, text, path))
    return list(best.values())

def regex_filter(rows: Iterable[tuple], pattern: str, flags: int = re.IGNORECASE):
    rx = re.compile(pattern, flags)
    return [r for r in rows if rx.search(r[2])]
