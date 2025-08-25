# app/searcher.py
import os, sqlite3, re
from typing import Iterable, Optional, List

def _norm_prefix(p: str) -> str:
    absp = os.path.abspath(p)
    if os.name == "nt": absp = os.path.normcase(absp)
    sep = "\\" if os.name == "nt" else "/"
    return absp if absp.endswith(sep) else absp + sep

def _in_scopes(path: str, prefixes: List[str]) -> bool:
    if not prefixes: return True
    norm = os.path.abspath(path)
    if os.name == "nt": norm = os.path.normcase(norm)
    return any(norm.startswith(pref) for pref in prefixes)

def _normalize_fts_query(q: str) -> Optional[str]:
    """
    Returns:
      None  -> show-all mode (skip MATCH)
      str   -> safe FTS query (phrase-quoted unless user provided FTS syntax)
    """
    if not q or not q.strip():
        return None
    s = q.strip()
    # If user entered only wildcards -> show-all
    if all(ch in "*%?" for ch in s):
        return None
    # If user used FTS syntax, pass through
    ops = (' NEAR ', ' AND ', ' OR ', ' NOT ', '(', ')', '"')
    if any(op in s for op in ops):
        return s
    # Otherwise quote as a phrase so special chars don't break MATCH
    return f"\"{s.replace('\"', '\"\"')}\""

def fts(
    con: sqlite3.Connection,
    q: str,
    top_k: int = 200,
    *,
    path_prefixes: Optional[List[str]] = None,
    min_ts: Optional[int] = None,           # optional time cutoff (epoch seconds)
    time_field: str = "modified",           # "modified" or "created"
) -> list[tuple]:
    """
    Returns best chunk per file. Row: (chunk_id, ord, text, path)
    """
    cur = con.cursor()
    col = "f.mtime" if time_field == "modified" else "f.created_at"
    qn = _normalize_fts_query(q)

    if qn is None:
        # Show-all: newest files' first chunk (ord=0), scoped and time-filtered
        sql = f"""SELECT c.id
                  FROM chunks c
                  JOIN files f ON f.id = c.file_id
                  WHERE c.ord = 0
                    {"AND " + col + " >= ? " if min_ts is not None else ""}
                  ORDER BY {col} DESC
                  LIMIT ?"""
        params = ([min_ts] if min_ts is not None else []) + [top_k]
        cur.execute(sql, tuple(params))
        ids = [r[0] for r in cur.fetchall()]
    else:
        # Normal FTS query
        sql = f"""SELECT m.chunk_id, bm25(fts) AS score
                  FROM fts
                  JOIN fts_map m ON m.rowid = fts.rowid
                  JOIN chunks c  ON c.id    = m.chunk_id
                  JOIN files  f  ON f.id    = c.file_id
                  WHERE fts MATCH ?
                    {"AND " + col + " >= ? " if min_ts is not None else ""}
                  ORDER BY score
                  LIMIT ?"""
        params = [qn] + ([min_ts] if min_ts is not None else []) + [top_k]
        cur.execute(sql, tuple(params))
        ids = [r[0] for r in cur.fetchall()]

    if not ids:
        return []

    ph = ",".join("?" * len(ids))
    cur.execute(f"""SELECT c.id, c.ord, c.text, f.path
                    FROM chunks c JOIN files f ON c.file_id=f.id
                    WHERE c.id IN ({ph})""", ids)
    rows = cur.fetchall()

    # keep returned order
    order = {cid: i for i, cid in enumerate(ids)}
    rows.sort(key=lambda r: order.get(r[0], 1e9))

    # scope filter and best-per-file
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
