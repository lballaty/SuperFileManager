import os, sqlite3, threading, queue, tkinter as tk, tkinter.scrolledtext as sc
from tkinter import filedialog, messagebox
from . import db, indexer, searcher

IS_MAC = (os.uname().sysname == "Darwin")
DB_PATH = (os.path.expanduser("~/Library/Application Support/SuperFileManager/state.sqlite")
           if IS_MAC else os.path.expanduser("~/.local/share/SuperFileManager/state.sqlite"))
EXCLUDES = [".git","node_modules","dist","build","__pycache__","/proc","/sys","/dev","/Volumes",
            "C:\\Windows","C:\\Program Files","C:\\ProgramData"]

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SuperFileManager"); self.geometry("900x600")
        self.db_path = DB_PATH
        # UI thread connection (reads/search). Do not share across threads.
        self.con: sqlite3.Connection = db.connect(self.db_path)
        db.init(self.con)

        self.work_q: "queue.Queue[tuple[str,dict]]" = queue.Queue()
        self.worker: threading.Thread | None = None
        self._build()
        self.after(150, self._poll)

    def _build(self):
        top = tk.Frame(self); top.pack(fill="x", padx=8, pady=6)
        self.root_var = tk.StringVar(value=os.path.expanduser("~"))
        self.e_root = tk.Entry(top, textvariable=self.root_var, width=70); self.e_root.pack(side="left", padx=4)
        self.btn_choose = tk.Button(top, text="Choose…", command=self.choose_root); self.btn_choose.pack(side="left")
        self.btn_index = tk.Button(top, text="Index", command=self.index_threaded); self.btn_index.pack(side="left", padx=6)

        mid = tk.Frame(self); mid.pack(fill="x", padx=8)
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

    def choose_root(self):
        p = filedialog.askdirectory(initialdir=self.root_var.get())
        if p: self.root_var.set(p)

    # threaded indexing
    def index_threaded(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Index", "Index already running"); return
        root = self.root_var.get().strip()
        if not root or not os.path.isdir(root):
            messagebox.showerror("Error", "Invalid directory"); return
        self._lock_ui(True)
        self.status.config(text=f"Indexing {root} …")

        def progress(ev: dict):
            self.work_q.put(("progress", ev))

        def job():
            try:
                # Open a NEW connection in this worker thread
                wcon = db.connect(self.db_path, check_same_thread=False)
                db.init(wcon)
                indexer.index_root(wcon, root, EXCLUDES, progress_cb=progress, batch=200)
                wcon.close()
            finally:
                self.work_q.put(("done", {}))

        self.worker = threading.Thread(target=job, daemon=True)
        self.worker.start()

    def _lock_ui(self, busy: bool):
        state = "disabled" if busy else "normal"
        self.btn_index.config(state=state); self.btn_choose.config(state=state); self.e_root.config(state=state)

    def _poll(self):
        try:
            while True:
                what, data = self.work_q.get_nowait()
                if what == "progress":
                    f = data.get("files",0); c = data.get("chunks",0); s = data.get("secs",0)
                    self.status.config(text=f"Indexing… files={f} chunks={c} t={s}s")
                elif what == "done":
                    self.status.config(text="Index complete")
                    self._lock_ui(False)
        except queue.Empty:
            pass
        self.after(150, self._poll)

    def search(self):
        q = self.q_var.get().strip()
        if not q: return
        rows = searcher.fts(self.con, q, top_k=500)
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

def main(): App().mainloop()
if __name__ == "__main__": main()
