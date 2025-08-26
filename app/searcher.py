# app/searcher.py
import os, sqlite3, re
from typing import Iterable, Optional, List
from .logging_conf import get_logger
log = get_logger("searcher")


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
    min_ts: Optional[int] = None,           # epoch seconds
    time_field: str = "modified",           # "modified" or "created"
) -> list[tuple]:
    cur = con.cursor()
    col = "f.mtime" if time_field == "modified" else "COALESCE(f.created_at, f.mtime)"
    qn = _normalize_fts_query(q)
    log.debug(f"fts args q={q!r} top_k={top_k} min_ts={min_ts} field={time_field} scopes={len(path_prefixes or [])}")

    # Build scope SQL
    prefixes = [_norm_prefix(p) for p in (path_prefixes or [])]
    scope_sql = ""
    scope_params: List[str] = []
    if prefixes:
        scope_sql = "(" + " OR ".join(["f.path LIKE ?"] * len(prefixes)) + ")"
        scope_params = [p + "%" for p in prefixes]

    if qn is None:
        where = ["c.ord = 0"]
        params: List[object] = []
        if min_ts is not None:
            where.append(f"{col} >= ?"); params.append(min_ts)
        if scope_sql:
            where.append(scope_sql); params.extend(scope_params)
        sql = f"""SELECT c.id
                  FROM chunks c
                  JOIN files f ON f.id = c.file_id
                  WHERE {" AND ".join(where) if where else "1=1"}
                  ORDER BY {col} DESC
                  LIMIT ?"""
        params.append(top_k)
        log.debug("WHERE=%s params=%s", " AND ".join(where), params)
        cur.execute(sql, tuple(params))
        ids = [r[0] for r in cur.fetchall()]
    else:
        where = ["fts MATCH ?"]
        params: List[object] = [qn]
        if min_ts is not None:
            where.append(f"{col} >= ?"); params.append(min_ts)
        if scope_sql:
            where.append(scope_sql); params.extend(scope_params)
        sql = f"""SELECT m.chunk_id, bm25(fts) AS score
                  FROM fts
                  JOIN fts_map m ON m.rowid = fts.rowid
                  JOIN chunks c  ON c.id    = m.chunk_id
                  JOIN files  f  ON f.id    = c.file_id
                  WHERE {" AND ".join(where)}
                  ORDER BY score
                  LIMIT ?"""
        params.append(top_k)
        log.debug("WHERE=%s params=%s", " AND ".join(where), params)
        cur.execute(sql, tuple(params))
        ids = [r[0] for r in cur.fetchall()]

    if not ids:
        log.debug("fts ids=0; files=0")
        return []

    ph = ",".join("?" * len(ids))
    cur.execute(f"""SELECT c.id, c.ord, c.text, f.path
                    FROM chunks c JOIN files f ON c.file_id=f.id
                    WHERE c.id IN ({ph})""", ids)
    rows = cur.fetchall()

    order = {cid: i for i, cid in enumerate(ids)}
    rows.sort(key=lambda r: order.get(r[0], 1e9))

    best = {}
    for cid, ord_, text, path in rows:
        best.setdefault(path, (cid, ord_, text, path))

    log.debug("fts ids=%d; files=%d", len(ids), len(best))
    return list(best.values())


def regex_filter(rows: Iterable[tuple], pattern: str, flags: int = re.IGNORECASE):
    rx = re.compile(pattern, flags)
    return [r for r in rows if rx.search(r[2])]
