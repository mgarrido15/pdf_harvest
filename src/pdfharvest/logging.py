# src/pdfharvest/logging.py
import logging

def setup_logging(level: int = logging.INFO) -> None:
    """Configure basic structured logging for the application."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
