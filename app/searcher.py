# app/searcher.py
import sqlite3, re
from typing import Iterable

def fts(con: sqlite3.Connection, q: str, top_k: int = 500) -> list[tuple]:
    """
    Run an FTS5 query and return matching chunks.

    Returns list of tuples:
        (chunk_id, ord, text, path)
    """
    sql = """SELECT m.chunk_id, bm25(fts) AS score
             FROM fts
             JOIN fts_map m ON m.rowid = fts.rowid
             WHERE fts MATCH ?
             ORDER BY score
             LIMIT ?"""
    cur = con.cursor()
    cur.execute(sql, (q, top_k))
    ids = [row[0] for row in cur.fetchall()]
    if not ids:
        return []
    ph = ",".join("?" * len(ids))
    cur.execute(f"""SELECT c.id, c.ord, c.text, f.path
                    FROM chunks c
                    JOIN files f ON c.file_id = f.id
                    WHERE c.id IN ({ph})""", ids)
    rows = cur.fetchall()
    order = {cid: i for i, cid in enumerate(ids)}
    return sorted(rows, key=lambda r: order.get(r[0], 1e9))

def regex_filter(rows: Iterable[tuple], pattern: str):
    """Filter FTS rows by regex match on chunk text."""
    rx = re.compile(pattern, re.IGNORECASE)
    return [r for r in rows if rx.search(r[2])]

