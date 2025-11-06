import logging
import time
from pathlib import Path
from pdfharvest.logging import setup_logging


def test_setup_logging_creates_log_file(tmp_path: Path):
    """
    Test that setup_logging creates a rotating file handler, sets correct log level,
    and writes logs to file.
    """
    cfg = {
        "logging": {
            "level": "DEBUG",
            "file": "test_harvest.log",
            "rotate_bytes": 1024 * 1024,
            "backup_count": 2
        }
    }

    out_dir = tmp_path / "output"
    out_dir.mkdir()
    logger = setup_logging(cfg, out_dir)
    assert isinstance(logger, logging.Logger)
    logger.debug("Debug message test")
    logger.info("Info message test")
    logger.warning("Warning message test")
    for handler in logger.handlers:
        handler.flush()
    time.sleep(0.1)
    log_file = out_dir / "logs" / "test_harvest.log"
    assert log_file.exists(), "El archivo de log no fue creado"
    content = log_file.read_text(encoding="utf-8")
    assert "Debug message test" in content, f"Contenido:\n{content}"
    assert "Info message test" in content
    assert "Warning message test" in content
    assert "INFO" in content or "DEBUG" in content, "No se encontró el nivel en el log"
