# app/main.py — adds Cancel + knobs UI + passes through to indexer

import os, sqlite3, threading, queue, tkinter as tk, tkinter.scrolledtext as sc
from tkinter import filedialog, messagebox
from . import db, indexer, searcher

IS_MAC = (os.uname().sysname == "Darwin")
DB_PATH = (os.path.expanduser("~/Library/Application Support/SuperFileManager/state.sqlite")
           if IS_MAC else os.path.expanduser("~/.local/share/SuperFileManager/state.sqlite"))

EXCLUDES = [".git","node_modules","dist","build","__pycache__",
            "/proc","/sys","/dev","/Volumes",
            "C:\\Windows","C:\\Program Files","C:\\ProgramData"]

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SuperFileManager"); self.geometry("1000x640")
        self.db_path = DB_PATH
        self.con: sqlite3.Connection = db.connect(self.db_path); db.init(self.con)
        try: db.migrate(self.con)
        except Exception: pass

        self.work_q: "queue.Queue[tuple[str,dict]]" = queue.Queue()
        self.worker: threading.Thread | None = None
        self.stop_evt: threading.Event | None = None

        self._build()
        self.after(150, self._poll)

    def _build(self):
        # top bar
        top = tk.Frame(self); top.pack(fill="x", padx=8, pady=6)
        self.root_var = tk.StringVar(value=os.path.expanduser("~"))
        self.e_root = tk.Entry(top, textvariable=self.root_var, width=70); self.e_root.pack(side="left", padx=4)
        self.btn_choose = tk.Button(top, text="Choose…", command=self.choose_root); self.btn_choose.pack(side="left")
        self.btn_index = tk.Button(top, text="Index", command=self.index_threaded); self.btn_index.pack(side="left", padx=6)
        self.btn_cancel = tk.Button(top, text="Cancel", command=self.cancel_index, state="disabled")
        self.btn_cancel.pack(side="left")

        # knobs row
        knobs = tk.Frame(self); knobs.pack(fill="x", padx=8)
        tk.Label(knobs, text="Reindex days").pack(side="left")
        self.reindex_days = tk.IntVar(value=14)
        tk.Spinbox(knobs, from_=0, to=365, width=5, textvariable=self.reindex_days).pack(side="left", padx=(4,12))

        tk.Label(knobs, text="Verify-hash days").pack(side="left")
        self.verify_days = tk.IntVar(value=7)
        tk.Spinbox(knobs, from_=0, to=365, width=5, textvariable=self.verify_days).pack(side="left", padx=(4,12))

        self.prune_var = tk.BooleanVar(value=False)
        tk.Checkbutton(knobs, text="Prune missing", variable=self.prune_var).pack(side="left", padx=6)

        self.fullhash_var = tk.BooleanVar(value=False)
        tk.Checkbutton(knobs, text="Full-hash large files", variable=self.fullhash_var).pack(side="left", padx=6)


        # stats row
        stats = tk.Frame(self); stats.pack(fill="x", padx=8, pady=(4,6))
        self.stats_var = tk.StringVar(value="Stats: n/a")
        tk.Label(stats, textvariable=self.stats_var).pack(side="left")
        tk.Button(stats, text="Refresh Stats", command=self.update_stats).pack(side="left", padx=8)


        # search row
        mid = tk.Frame(self); mid.pack(fill="x", padx=8, pady=(6,0))
        self.q_var = tk.StringVar()
        e = tk.Entry(mid, textvariable=self.q_var, width=60); e.pack(side="left", padx=4)
        e.bind("<Return>", lambda _e: self.search())
        tk.Button(mid, text="Search", command=self.search).pack(side="left")
        self.regex_var = tk.BooleanVar(); tk.Checkbutton(mid, text="Regex", variable=self.regex_var).pack(side="left")

        self.status = tk.Label(self, text="Ready"); self.status.pack(fill="x", padx=8)
        self.split = tk.PanedWindow(self, orient="horizontal"); self.split.pack(fill="both", expand=True, padx=8, pady=6)
        self.listbox = tk.Listbox(self.split, width=60); self.listbox.bind("<<ListboxSelect>>", self.show_preview)
        self.preview = sc.ScrolledText(self.split, wrap="word")
        self.split.add(self.listbox); self.split.add(self.preview)

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def choose_root(self):
        p = filedialog.askdirectory(initialdir=self.root_var.get())
        if p: self.root_var.set(p); self.update_stats()

    # ——— Indexing ———
    def index_threaded(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Index", "Index already running"); return
        root = self.root_var.get().strip()
        if not root or not os.path.isdir(root):
            messagebox.showerror("Error", "Invalid directory"); return

        self._lock_ui(True)
        self.status.config(text=f"Indexing {root} …")
        self.stop_evt = threading.Event()

        def progress(ev: dict):
            self.work_q.put(("progress", ev))

        def job():
            try:
                wcon = db.connect(self.db_path, check_same_thread=False); db.init(wcon)
                try: db.migrate(wcon)
                except Exception: pass
                indexer.index_root(
                    wcon, root, EXCLUDES,
                    progress_cb=progress, batch=200,
                    prune_missing=self.prune_var.get(),
                    reindex_days=int(self.reindex_days.get()),
                    verify_hash_days=int(self.verify_days.get()),
                    force_full_hash_large=bool(self.fullhash_var.get()),
                    stop_event=self.stop_evt
                )
                wcon.close()
            finally:
                self.work_q.put(("done", {}))

        self.worker = threading.Thread(target=job, daemon=True)
        self.worker.start()
        self.btn_cancel.config(state="normal")

    def cancel_index(self):
        if self.stop_evt: self.stop_evt.set()
        self.btn_cancel.config(state="disabled")

    def _lock_ui(self, busy: bool):
        state = "disabled" if busy else "normal"
        self.btn_index.config(state=state); self.btn_choose.config(state=state); self.e_root.config(state=state)
        if not busy: self.btn_cancel.config(state="disabled")

    def _poll(self):
        try:
            while True:
                what, data = self.work_q.get_nowait()
                if what == "progress":
                    f_seen = data.get("files_seen") or data.get("files", 0)
                    f_idx  = data.get("files_indexed", 0)
                    chunks = data.get("chunks", 0)
                    secs   = data.get("secs", 0)
                    self.status.config(text=f"Indexing… seen={f_seen} indexed={f_idx} chunks={chunks} t={secs}s")
                elif what == "done":
                    self.status.config(text="Index complete" + (" (cancelled)" if data.get("cancelled") else ""))
                    self._lock_ui(False)
                    self.update_stats()
        except queue.Empty:
            pass
        self.after(150, self._poll)
    # ——— /Indexing ———

    # ——— Search ———
    def search(self):
        q = self.q_var.get().strip()
        if not q: return
        rows = searcher.fts(self.con, q, top_k=200)
        if self.regex_var.get(): rows = searcher.regex_filter(rows, q)
        self.listbox.delete(0, tk.END); self.preview.delete("1.0", tk.END)
        for cid, ord_, text, path in rows[:300]:
            self.listbox.insert(tk.END, f"{path}  [chunk {ord_}]  {text[:120]}…")
        self.status.config(text=f"{min(300,len(rows))} results")

    def show_preview(self, _evt=None):
        sel = self.listbox.curselection()
        if not sel: return
        line = self.listbox.get(sel[0])
        self.preview.delete("1.0", tk.END); self.preview.insert("1.0", line)
    # ——— /Search ———

    def on_close(self):
        # best-effort cancel + close
        self.cancel_index()
        self.destroy()

    def update_stats(self):
        root = self.root_var.get().strip()
        if not root or not os.path.isdir(root):
            self.stats_var.set("Stats: invalid directory"); 
            return
        d = db.counts_for_root(self.con, root)
        from time import localtime, strftime
        ts = "—" if not d.get("last_indexed_at") else strftime("%Y-%m-%d %H:%M", localtime(d["last_indexed_at"]))
        self.stats_var.set(
            f"Stats for {root}: files={d['files_total']}  text_files={d['files_text']}  chunks={d['chunks']}  last_indexed={ts}"
            )
 

def main(): App().mainloop()
if __name__ == "__main__": main()
