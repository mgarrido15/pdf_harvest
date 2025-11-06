import logging
import logging.handlers
from pathlib import Path
from typing import Any, Dict

def setup_logging(cfg: Dict[str, Any], out_dir: Path):
    log_cfg = cfg.get("logging", {})
    level_name = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log_dir = out_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_cfg.get("file", "harvest.log")
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=int(log_cfg.get("rotate_bytes", 10_485_760)),
        backupCount=int(log_cfg.get("backup_count", 5)),
        encoding="utf-8",
    )
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger("harvest")
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False  
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger.info(f"Logging initialized → level={level_name}, file={log_file}")
    return logger


