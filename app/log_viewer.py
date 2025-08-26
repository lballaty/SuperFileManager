# app/log_viewer.py
import os, re, time, tkinter as tk, tkinter.scrolledtext as sc
from tkinter import messagebox
from .logging_conf import log_path

class LogViewer(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("SuperFileManager — Logs"); self.geometry("900x600")
        self.path = log_path()
        self.follow = tk.BooleanVar(value=True)
        self.regex = tk.BooleanVar(value=False)
        self.case = tk.BooleanVar(value=True)
        self.filter_var = tk.StringVar()
        self.status = tk.StringVar(value=self.path)
        self._mtime = 0
        self._build()
        self.refresh()

    def _build(self):
        bar = tk.Frame(self); bar.pack(fill="x", padx=8, pady=6)
        tk.Label(bar, text="Filter").pack(side="left")
        ent = tk.Entry(bar, textvariable=self.filter_var, width=40); ent.pack(side="left", padx=(4,8))
        ent.bind("<Return>", lambda _e: self.refresh())
        tk.Checkbutton(bar, text="Regex", variable=self.regex).pack(side="left")
        tk.Checkbutton(bar, text="Ignore case", variable=self.case).pack(side="left", padx=(6,8))
        tk.Button(bar, text="Apply", command=self.refresh).pack(side="left")
        tk.Button(bar, text="Refresh", command=self.refresh).pack(side="left", padx=(6,0))
        tk.Checkbutton(bar, text="Follow", variable=self.follow, command=self._schedule).pack(side="left", padx=(12,0))
        tk.Button(bar, text="Open file…", command=lambda: messagebox.showinfo("Log file", self.path)).pack(side="right")

        self.text = sc.ScrolledText(self, wrap="none")
        self.text.pack(fill="both", expand=True, padx=8, pady=(0,6))
        self.text.configure(state="disabled")
        tk.Label(self, textvariable=self.status, anchor="w").pack(fill="x", padx=8, pady=(0,6))

        self.filter_var.trace_add("write", lambda *_: self._schedule(delay=0.3))

    def _read(self, max_bytes=2_000_000):
        try:
            size = os.path.getsize(self.path)
            with open(self.path, "rb") as f:
                if size > max_bytes:
                    f.seek(-max_bytes, os.SEEK_END)
                data = f.read()
            return data.decode("utf-8", errors="replace").splitlines()
        except Exception as e:
            return [f"[log open error: {e}]"]

    def _filter_lines(self, lines):
        pat = self.filter_var.get().strip()
        if not pat:
            return lines
        if self.regex.get():
            flags = re.IGNORECASE if self.case.get() else 0
            try:
                rx = re.compile(pat, flags)
            except re.error as e:
                self.status.set(f"Invalid regex: {e}")
                return lines
            return [ln for ln in lines if rx.search(ln)]
        else:
            needle = pat.lower() if self.case.get() else pat
            out = []
            if self.case.get():
                for ln in lines:
                    if needle in ln.lower(): out.append(ln)
            else:
                for ln in lines:
                    if needle in ln: out.append(ln)
            return out

    def refresh(self):
        try: self._mtime = os.path.getmtime(self.path)
        except Exception: self._mtime = 0
        lines = self._filter_lines(self._read())
        text = "\n".join(lines)
        self.text.configure(state="normal"); self.text.delete("1.0", "end"); self.text.insert("1.0", text)
        self.text.configure(state="disabled"); self.text.see("end")
        self.status.set(f"{self.path} — {len(lines)} lines shown")
        self._schedule()

    def _tick(self):
        if not self.follow.get(): return
        try: m = os.path.getmtime(self.path)
        except Exception: m = 0
        if m != self._mtime:
            self.refresh()
        else:
            self._schedule()

    def _schedule(self, delay=1.0):
        if self.follow.get():
            self.after(int(delay*1000), self._tick)
