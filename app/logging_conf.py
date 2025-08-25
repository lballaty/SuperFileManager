# app/logging_conf.py
import logging, os, pathlib
from logging.handlers import RotatingFileHandler

def _data_dir():
    if os.uname().sysname == "Darwin":
        base = os.path.expanduser("~/Library/Application Support/SuperFileManager")
    else:
        base = os.path.expanduser("~/.local/share/SuperFileManager")
    pathlib.Path(base, "logs").mkdir(parents=True, exist_ok=True)
    return os.path.join(base, "logs", "sfm.log")

_logger = None

def get_logger(name="SFM", level=logging.WARNING):
    """Singleton base logger; returns a child logger."""
    global _logger
    if _logger is None:
        log_path = _data_dir()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        root = logging.getLogger("SFM")
        root.setLevel(level)
        fh = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=3)
        fh.setFormatter(fmt); fh.setLevel(level)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt); sh.setLevel(level)
        root.addHandler(fh); root.addHandler(sh)
        _logger = root
    # update levels on toggle
    _logger.setLevel(level)
    for h in _logger.handlers: h.setLevel(level)
    return _logger.getChild(name)

def log_path():
    return _data_dir()
