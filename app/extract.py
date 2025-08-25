import os, re, chardet

TEXT_EXT = {".txt",".md",".py",".js",".ts",".json",".yaml",".yml",".html",".htm",
            ".css",".sql",".ini",".cfg",".log",".csv",".tsv",".toml"}

def is_textable(path: str) -> bool:
    return os.path.splitext(path.lower())[1] in TEXT_EXT

def read_text(path: str, max_bytes: int = 200_000) -> str | None:
    try:
        with open(path, "rb") as f: raw = f.read(max_bytes)
    except Exception:
        return None
    enc = chardet.detect(raw).get("encoding") or "utf-8"
    try: s = raw.decode(enc, errors="ignore")
    except Exception: s = raw.decode("utf-8", errors="ignore")
    if path.lower().endswith((".html",".htm")):
        s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def chunk(text: str, target: int = 4096):
    out=[]; i=0; ord_=0
    while i < len(text):
        out.append((ord_, text[i:i+target], i, min(len(text), i+target)))
        i += target; ord_ += 1
    return out

