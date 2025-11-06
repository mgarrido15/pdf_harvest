import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any

def setup_logging(cfg: Dict[str, Any], out_dir: Path):
    log_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    logs_dir = (out_dir / "logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = (logs_dir / log_cfg.get("file", "harvest.log")).resolve()
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=int(log_cfg.get("rotate_bytes", 10_485_760)),
        backupCount=int(log_cfg.get("backup_count", 5)),
        encoding="utf-8"
    )

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=level, format=fmt, handlers=[handler, logging.StreamHandler()])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("harvest")



