# app/main.py
import os, sqlite3, threading, queue, tkinter as tk, tkinter.scrolledtext as sc
from tkinter import filedialog, messagebox
import re, time
from . import db, indexer, searcher

IS_MAC = (os.uname().sysname == "Darwin")
DB_PATH = (os.path.expanduser("~/Library/Application Support/SuperFileManager/state.sqlite")
           if IS_MAC else os.path.expanduser("~/.local/share/SuperFileManager/state.sqlite"))

CREATED_SUPPORTED = IS_MAC or (os.name == "nt")


EXCLUDES = [".git","node_modules","dist","build","__pycache__",
            "/proc","/sys","/dev","/Volumes",
            "C:\\Windows","C:\\Program Files","C:\\ProgramData"]

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SuperFileManager"); self.geometry("1100x680")
        self.db_path = DB_PATH
        self.con: sqlite3.Connection = db.connect(self.db_path); db.init(self.con)
        try: db.migrate(self.con)
        except Exception: pass

        self.work_q: "queue.Queue[tuple[str,dict]]" = queue.Queue()
        self.worker: threading.Thread | None = None
        self.stop_evt: threading.Event | None = None

        # load search scopes from settings
        self.scopes = db.get_setting(self.con, "search_scopes", [])

        self._build()
        self.after(150, self._poll)
        self.update_stats()

    def _build(self):
        # top: root + index controls
        top = tk.Frame(self); top.pack(fill="x", padx=8, pady=6)
        self.root_var = tk.StringVar(value=os.path.expanduser("~"))
        self.e_root = tk.Entry(top, textvariable=self.root_var, width=70); self.e_root.pack(side="left", padx=4)
        self.btn_choose = tk.Button(top, text="Choose…", command=self.choose_root); self.btn_choose.pack(side="left")
        self.btn_index = tk.Button(top, text="Index", command=self.index_threaded); self.btn_index.pack(side="left", padx=6)
        self.btn_cancel = tk.Button(top, text="Cancel", command=self.cancel_index, state="disabled")
        self.btn_cancel.pack(side="left")

        # knobs row (reindex + verify + prune + fullhash)
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

        # search row + regex builder
        mid = tk.Frame(self); mid.pack(fill="x", padx=8, pady=(6,0))
        self.q_var = tk.StringVar()
        e = tk.Entry(mid, textvariable=self.q_var, width=60); e.pack(side="left", padx=4)
        e.bind("<Return>", lambda _e: self.search())
        tk.Button(mid, text="Search", command=self.search).pack(side="left")
        self.regex_var = tk.BooleanVar()
        tk.Checkbutton(mid, text="Regex", variable=self.regex_var).pack(side="left", padx=(8,0))
        tk.Button(mid, text="Regex Builder…", command=self.open_regex_builder).pack(side="left", padx=6)

        # time filter row
        trow = tk.Frame(self); trow.pack(fill="x", padx=8, pady=(4,0))
        tk.Label(trow, text="Time filter").pack(side="left")

        self.time_preset = tk.StringVar(value="All")
        presets = ["All","Last 24 hours","Last 7 days","Last 30 days","Custom"]
        tk.OptionMenu(trow, self.time_preset, *presets).pack(side="left", padx=6)

        # custom range controls
        self.custom_num = tk.IntVar(value=7)
        self.custom_unit = tk.StringVar(value="days")
        units = ["minutes","hours","days","weeks","months","years"]
        tk.Spinbox(trow, from_=1, to=9999, width=6, textvariable=self.custom_num).pack(side="left")
        tk.OptionMenu(trow, self.custom_unit, *units).pack(side="left", padx=6)

        # primary time field
        tk.Label(trow, text="Field").pack(side="left", padx=(12,2))
        self.time_field = tk.StringVar(value="modified")
        tk.OptionMenu(trow, self.time_field, "modified","created").pack(side="left")

        # fallback notice
        self.fallback_var = tk.StringVar(value="")
        tk.Label(trow, textvariable=self.fallback_var, fg="orange").pack(side="left", padx=12)

        # scope selector
        scope = tk.Frame(self); scope.pack(fill="x", padx=8, pady=(8,4))
        tk.Label(scope, text="Search scope (directories, recursive):").pack(side="left")
        self.scope_list = tk.Listbox(scope, height=3, width=80)
        self.scope_list.pack(side="left", padx=8)
        btns = tk.Frame(scope); btns.pack(side="left")
        tk.Button(btns, text="Add Dir…", command=self.add_scope_dir).pack(fill="x", pady=1)
        tk.Button(btns, text="Remove", command=self.remove_scope).pack(fill="x", pady=1)
        tk.Button(btns, text="Clear", command=self.clear_scopes).pack(fill="x", pady=1)
        for p in self.scopes: self.scope_list.insert(tk.END, p)

        # stats row
        stats = tk.Frame(self); stats.pack(fill="x", padx=8, pady=(4,6))
        self.stats_var = tk.StringVar(value="Stats: n/a")
        tk.Label(stats, textvariable=self.stats_var).pack(side="left")
        tk.Button(stats, text="Refresh Stats", command=self.update_stats).pack(side="left", padx=8)

        # results + preview
        self.status = tk.Label(self, text="Ready"); self.status.pack(fill="x", padx=8)
        self.split = tk.PanedWindow(self, orient="horizontal"); self.split.pack(fill="both", expand=True, padx=8, pady=6)
        self.listbox = tk.Listbox(self.split, width=60); self.listbox.bind("<<ListboxSelect>>", self.show_preview)
        self.preview = sc.ScrolledText(self.split, wrap="word")
        self.split.add(self.listbox); self.split.add(self.preview)

        self.protocol("WM_DELETE_WINDOW", self.on_close)


    def _compute_min_ts(self) -> tuple[int|None, str]:
        p = self.time_preset.get()
        if p == "All":
            return None, self.time_field.get()
        if p == "Last 24 hours":
            return int(time.time()) - 24*3600, self.time_field.get()
        if p == "Last 7 days":
            return int(time.time()) - 7*24*3600, self.time_field.get()
        if p == "Last 30 days":
            return int(time.time()) - 30*24*3600, self.time_field.get()
        # Custom
        n = max(1, int(self.custom_num.get()))
        mul = {"minutes":60, "hours":3600, "days":86400,
            "weeks":7*86400, "months":30*86400, "years":365*86400}[self.custom_unit.get()]
        return int(time.time()) - n*mul, self.time_field.get()


    # —— scope handlers ——
    def add_scope_dir(self):
        p = filedialog.askdirectory(initialdir=os.path.expanduser("~"))
        if not p: return
        ap = os.path.abspath(p)
        if ap not in self.scopes:
            self.scopes.append(ap)
            self.scope_list.insert(tk.END, ap)
            db.set_setting(self.con, "search_scopes", self.scopes)

    def remove_scope(self):
        sel = self.scope_list.curselection()
        if not sel: return
        idx = sel[0]
        val = self.scope_list.get(idx)
        self.scope_list.delete(idx)
        self.scopes = [p for p in self.scopes if p != val]
        db.set_setting(self.con, "search_scopes", self.scopes)

    def clear_scopes(self):
        self.scope_list.delete(0, tk.END)
        self.scopes = []
        db.set_setting(self.con, "search_scopes", self.scopes)

    # —— index controls ——
    def choose_root(self):
        p = filedialog.askdirectory(initialdir=self.root_var.get())
        if p:
            self.root_var.set(p)
            self.update_stats()

    def index_threaded(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Index", "Index already running"); return
        root = self.root_var.get().strip()
        if not root or not os.path.isdir(root):
            messagebox.showerror("Error", "Invalid directory"); return

        self._lock_ui(True)
        self.status.config(text=f"Indexing {root} …")
        self.stop_evt = threading.Event()

        def progress(ev: dict): self.work_q.put(("progress", ev))

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

    # —— search ——
    def search(self):
        q = self.q_var.get().strip()
        if not q:
            return

        # compute time lower bound and chosen field
        min_ts, field = self._compute_min_ts()

        # created→modified fallback notice
        used_field = field
        self.fallback_var.set("")
        if field == "created" and not CREATED_SUPPORTED:
            used_field = "modified"
            self.fallback_var.set("Created time unsupported on this OS. Using modified.")

        # run search with scopes + time filter
        rows = searcher.fts(
            self.con, q, top_k=200,
            path_prefixes=self.scopes,
            min_ts=min_ts,
            time_field=used_field,
        )

        if self.regex_var.get():
            rows = searcher.regex_filter(rows, q)

        self.listbox.delete(0, tk.END)
        self.preview.delete("1.0", tk.END)
        for cid, ord_, text, path in rows[:300]:
            self.listbox.insert(tk.END, f"{path}  [chunk {ord_}]  {text[:120]}…")
        self.status.config(text=f"{min(300, len(rows))} results")


    def show_preview(self, _evt=None):
        sel = self.listbox.curselection()
        if not sel: return
        line = self.listbox.get(sel[0])
        self.preview.delete("1.0", tk.END); self.preview.insert("1.0", line)

    # —— regex builder ——
    def open_regex_builder(self):
        w = tk.Toplevel(self); w.title("Regex Builder"); w.geometry("700x420")
        frm = tk.Frame(w); frm.pack(fill="x", padx=10, pady=8)
        tk.Label(frm, text="Literal text").grid(row=0, column=0, sticky="w")
        lit = tk.Entry(frm, width=50); lit.grid(row=0, column=1, columnspan=4, sticky="we", padx=6)

        ci = tk.BooleanVar(value=True)
        ww = tk.BooleanVar(value=False)
        starts = tk.BooleanVar(value=False)
        ends   = tk.BooleanVar(value=False)
        ml = tk.BooleanVar(value=False)
        ds = tk.BooleanVar(value=False)

        tk.Checkbutton(frm, text="Case-insensitive", variable=ci).grid(row=1, column=0, sticky="w")
        tk.Checkbutton(frm, text="Whole word", variable=ww).grid(row=1, column=1, sticky="w")
        tk.Checkbutton(frm, text="Starts ^", variable=starts).grid(row=1, column=2, sticky="w")
        tk.Checkbutton(frm, text="Ends $", variable=ends).grid(row=1, column=3, sticky="w")
        tk.Checkbutton(frm, text="Multiline", variable=ml).grid(row=1, column=4, sticky="w")
        tk.Checkbutton(frm, text="Dot matches newline", variable=ds).grid(row=1, column=5, sticky="w")

        tk.Label(frm, text="Sample text (optional)").grid(row=2, column=0, sticky="w", pady=(8,0))
        sample = sc.ScrolledText(w, height=10, wrap="word"); sample.pack(fill="both", expand=True, padx=10, pady=(0,8))
        status = tk.Label(w, text=""); status.pack(fill="x", padx=10)

        def build_pattern():
            s = re.escape(lit.get())
            if ww.get(): s = rf"\b{s}\b"
            if starts.get(): s = r"^" + s
            if ends.get(): s = s + r"$"
            return s

        def preview():
            pat = build_pattern()
            flags = 0
            if ci.get(): flags |= re.IGNORECASE
            if ml.get(): flags |= re.MULTILINE
            if ds.get(): flags |= re.DOTALL
            txt = sample.get("1.0", "end-1c")
            try:
                rx = re.compile(pat, flags)
                n = len(list(rx.finditer(txt)))
                status.config(text=f"Pattern: {pat}   Matches: {n}")
            except re.error as e:
                status.config(text=f"Invalid regex: {e}")

        def insert_into_search():
            pat = build_pattern()
            self.q_var.set(pat)
            self.regex_var.set(True)
            w.destroy()

        btnrow = tk.Frame(w); btnrow.pack(fill="x", padx=10, pady=6)
        tk.Button(btnrow, text="Preview", command=preview).pack(side="left")
        tk.Button(btnrow, text="Use in Search", command=insert_into_search).pack(side="right")

    # —— stats + close ——
    def update_stats(self):
        root = self.root_var.get().strip()
        if not root or not os.path.isdir(root):
            self.stats_var.set("Stats: invalid directory"); return
        d = db.counts_for_root(self.con, root)
        from time import localtime, strftime
        ts = "—" if not d.get("last_indexed_at") else strftime("%Y-%m-%d %H:%M", localtime(d["last_indexed_at"]))
        self.stats_var.set(f"Stats for {root}: files={d['files_total']}  text_files={d['files_text']}  chunks={d['chunks']}  last_indexed={ts}")

    def on_close(self):
        self.cancel_index()
        self.destroy()

def main(): App().mainloop()
if __name__ == "__main__": main()
